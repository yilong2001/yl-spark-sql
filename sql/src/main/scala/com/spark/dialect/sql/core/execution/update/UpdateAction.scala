package com.spark.dialect.sql.core.execution.update

import java.io.File
import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, UUID}

import com.spark.dialect.sql.core.catalyst.optimizer.UpdateOperationType.UpdateOperationType
import com.spark.dialect.sql.core.exception.AnalysisException
import com.spark.dialect.sql.core.execution.command.TableMetadataHelper.createTableDescByLike
import com.spark.dialect.sql.util.MetaConstants
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.types.StructType

import scala.util.control.NonFatal

/**
  * Created by yilong on 2019/7/4.
  */
trait UpdateSaveAction {
  def save(childRDD: RDD[Row], updateOperationType: UpdateOperationType)
}

abstract class BaseUpdateAction(spark: SparkSession,
                            targetTable: CatalogTable,
                            outputSchema: StructType) extends UpdateSaveAction {
  def save(childRDD: RDD[Row], updateOperationType: UpdateOperationType)
}

case class HiveUpdateAction(spark: SparkSession,
                       targetTable: CatalogTable,
                       outputSchema: StructType) extends BaseUpdateAction(spark,targetTable,outputSchema) {

  def updateViewTable(tempId: String, childRDD: RDD[Row]) : Unit = {
    val tempTable = createTempTargetTable(targetTable, CatalogTableType.MANAGED, tempId)

    val dataframe = spark.sqlContext.createDataFrame(childRDD, outputSchema)
    dataframe.write.insertInto(tempTable.identifier.identifier)
  }

  def getUserHome(fs : FileSystem) : String = {
    if (fs.getUri.toString.startsWith("hdfs://")) {
      fs.getWorkingDirectory.toUri.getPath
    } else {
      val userHome = if (!UserGroupInformation.isSecurityEnabled) {
        "/anonymous"
      } else {
        "/user/"+UserGroupInformation.getCurrentUser.getShortUserName
      }
      userHome
    }
  }

  def createTempTargetTable(origin: CatalogTable, tableType:CatalogTableType, tempId:String) : CatalogTable = {
    val target = origin.identifier
    val tempTarget = target.copy(table = target.table+"_"+tempId, database = target.database)

    val hadoopConf = spark.sessionState.newHadoopConf()
    val fileSystem = FileSystem.get(hadoopConf)

    val hdfspath = fileSystem.getWorkingDirectory+"/temp_table/"+tempTarget.identifier
    val targetDir = origin.tableType match {
      case CatalogTableType.VIEW => None
      case _ => Some(hdfspath)
    } //Some(new Path(wh+"/"+tempTarget.identifier).toUri.toString)

    val tempTargetTable = createTableDescByLike(spark, target, tempTarget, false, CatalogTableType.MANAGED, targetDir)
    val createTable = CreateTable(tempTargetTable, SaveMode.ErrorIfExists, None)
    val queryExec = new QueryExecution(spark,createTable).executedPlan
    queryExec.executeCollect()

    tempTargetTable
  }

  def deleteTempTargetTable(temp: CatalogTable) : Unit = {
    spark.sessionState.catalog.dropTable(temp.identifier, true, false)
  }

  def getTableLocation(table: CatalogTable) : String = {
    if (table.storage.locationUri.nonEmpty) {
      org.apache.spark.sql.catalyst.catalog.CatalogUtils.URIToString(table.storage.locationUri.get)
    } else {
      val hadoopConf = spark.sessionState.newHadoopConf()
      val fileSystem = FileSystem.get(hadoopConf)

      val p = fileSystem.getUri.toString+"/"+spark.conf.getAll.get("hive.metastore.warehouse.dir").getOrElse("/user/hive/warehouse")+
        "/"+table.identifier.identifier
      p
    }
  }

  override def save(childRDD: RDD[Row], updateOperationType: UpdateOperationType): Unit = {
    val target = targetTable.identifier

    val tempId = UUID.randomUUID().toString.replace("-","_")
    val curDate = System.currentTimeMillis()

    val hadoopConf = spark.sessionState.newHadoopConf()
    val fileSystem = FileSystem.get(hadoopConf)

    if (targetTable.tableType.name.equals(CatalogTableType.VIEW.name)) {
      updateViewTable(tempId, childRDD)
    } else {
      val now = new Date();
      /* h 1-12输出格式: 2017-04-16 01:01:22 */
      val format1 = new SimpleDateFormat("yyyyMMddhhmmss");
      val nowstr = format1.format(now);

      import spark.implicits._
      implicit val encoder1 = org.apache.spark.sql.Encoders.kryo[UnsafeRow]
      implicit val encoder2 = org.apache.spark.sql.Encoders.kryo[InternalRow]
      implicit val encoder3 = org.apache.spark.sql.Encoders.kryo[Row]

      //TODO: (1) backup files of target table;

      val bkpathstr = MetaConstants.backupDirBeforeUpdate(getUserHome(fileSystem), tempId, nowstr)

      val tempTargetTable = createTempTargetTable(targetTable, CatalogTableType.MANAGED, tempId)
      //TODO: (2) 写入更新后的数据(临时表)
      val dataframe = spark.sqlContext.createDataFrame(childRDD, outputSchema)
      dataframe.write.insertInto(tempTargetTable.identifier.identifier)

      val targetLocation = getTableLocation(targetTable)
      val tempLocation = getTableLocation(tempTargetTable)
      //local file
      if (targetLocation.toString.startsWith("file:/")) {
        throw new AnalysisException(s" $target table  $targetLocation schema is not support!")
      } else if (targetLocation.toString.startsWith("hdfs:/")) {
        //TODO: (3) 移动原始数据到备份目录；移动临时表的数据到原始数据目录；
        val nowpath = new Path(targetLocation)
        val temppath = new Path(tempLocation)
        val bkpath = new Path(bkpathstr)
        try {
          val fs = nowpath.getFileSystem(hadoopConf)

          fs.mkdirs(bkpath)

          fs.listStatus(nowpath).foreach(fstastus => {
            fs.rename(fstastus.getPath, bkpath)
          })

          fs.listStatus(temppath).foreach(fstastus => {
            fs.rename(fstastus.getPath, nowpath)
          })
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(s"Failed to  table $targetTable when removing data from : $nowpath " +
                s"because of ${e.toString}")
        }
      } else {
        throw new AnalysisException(s" $target table  $targetLocation schema is not support!")
      }

      //TODO: (4) 更新权限
      //TODO: (5) refresh (partitions) target table
      spark.sessionState.refreshTable(target.unquotedString)
      //TODO: (5) 删除临时表
      deleteTempTargetTable(tempTargetTable)
    }
  }
}
