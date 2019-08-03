package com.spark.dialect.sql.core.execution.command

import java.util.Locale

import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.execution.command.{CreateTableLikeCommand, DDLUtils, DataWritingCommand, RunnableCommand}
import com.spark.dialect.sql.core.catalyst.parser._
import com.spark.dialect.sql.core.exception.AnalysisException
import com.spark.dialect.sql.util.{MetaConstants, StrUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, EqualTo, Expression, Literal, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, LogicalRelation}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
/**
  * Created by yilong on 2019/1/6.
  */


/**
  * A command to create a snapshot with the same definition of the given existing table.
  * A CatalogTable are used: record snapshot info(dbtype, user, db, table(partitions), snapshot, datetime, etc)
  *
  * In the target snapshot with different scene:
  * (1) hive : just copy data (or partition data)
  *    if source table location is /parentPath/source::tableIdentifier
  *    then snapshot location is /parentPath/snapshot/target::tableIdentifier
  * (2) hbase : has a snapshot with name targetIdentifier
  * (3) solr : create snapshot with
  * (4) es / kudu / ... :
  *
  * The CatalogTable attributes copied from the source table are storage(inputFormat, outputFormat,
  * serde, compressed, properties), schema, provider, partitionColumnNames, bucketSpec.
  *
  * The syntax of using this command in SQL is:
  * {{{
  *   MAKE SNAPSHOT (IF NOT EXISTS)? target=tableIdentifier
  *     FROM source=tableIdentifier partitionSpec?
  * }}}
  */

object TableMetadataHelper {
  def createTableDescByLike(sparkSession : SparkSession,
                            sourceTable : TableIdentifier,
                            targetTable: TableIdentifier,
                            isLocal: Boolean,
                            targetTableType: CatalogTableType,
                            targetDir: Option[String]) : CatalogTable = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (targetDir.isEmpty) CatalogTableType.MANAGED else CatalogTableType.EXTERNAL

    CatalogTable(
      identifier = targetTable,
      tableType = tblType,
      storage = sourceTableDesc.storage.copy(locationUri = targetDir.map(CatalogUtils.stringToURI(_))),
      schema = sourceTableDesc.schema,
      provider = newProvider,
      partitionColumnNames = sourceTableDesc.partitionColumnNames,
      bucketSpec = sourceTableDesc.bucketSpec)
  }
}

object UniSnapshot {
  def build(sparkSession: SparkSession,
            sourceTable: TableIdentifier,
            partitions: java.util.List[TablePartitionSpec],
            targetTable: TableIdentifier,
            isLocal : Boolean,
            dir: String) : LogicalPlan = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)
    val storageType = sourceTableDesc.properties.get(MetaConstants.META_DATA_STORAGE_TYPE)
    if (storageType == null || storageType.equals(Some(MetaConstants.META_DATA_STORAGE_TYPE_HIVE))) {
      HiveSnapshotBaseBuilder(sparkSession).build(sourceTable, partitions, targetTable, isLocal, dir)
    } else if (storageType.equals(Some(MetaConstants.META_DATA_STORAGE_TYPE_HBASE))) {
      val zk = sourceTableDesc.properties.get(MetaConstants.META_DATA_HBASE_ZOOKEEPER).get
      val zkPort = sourceTableDesc.properties.get(MetaConstants.META_DATA_HBASE_ZOOKEEPER_PORT).get
      val hbaseTableName  = sourceTableDesc.properties.get(MetaConstants.META_DATA_HBASE_TABLE_NAME).get

      HBaseSnapshotCommand(zk,zkPort,hbaseTableName,sourceTable, partitions, targetTable)
    } else {
      HiveSnapshotBaseBuilder(sparkSession).build(sourceTable, partitions, targetTable, isLocal, dir)
    }
  }
}

trait SnapshotBaseBuilder {
  def build(sourceTable: TableIdentifier,
            partitions: java.util.List[TablePartitionSpec],
            targetTable: TableIdentifier,
            isLocal : Boolean,
            dir: String) : LogicalPlan

  def checkPartitionSpec(sourceTableDesc : CatalogTable, partitions: java.util.List[TablePartitionSpec]) : Unit = {
    if (sourceTableDesc.partitionColumnNames.nonEmpty) {
      partitions.asScala.foreach(partition =>
        if (partition.nonEmpty) {
          partition.keySet.foreach { colName =>
            if (!sourceTableDesc.partitionColumnNames.contains(colName)) {
              throw new AnalysisException(s"Make Snapshot from source table ${sourceTableDesc.identifier} is partitioned, " +
                s"but the specified partition spec refers to a column that is not partitioned: " +
                s"'$colName'")
            }
          }

          sourceTableDesc.partitionColumnNames
            .map(colName=>partition.contains(colName))
            .map(b=>{b match {
              case true => 1
              case _ => 0
            }}).foldLeft(1) {(l,r)=>
            if (l<r) {
              throw new AnalysisException(s"Make Snapshot from source table ${sourceTableDesc.identifier} is partitioned, " +
                "but the specified partition columns is not continued or not started from first partition column")
            }
            l
          }
        }
      )
    } else {
      if (partitions.size() > 0) {
        throw new AnalysisException(s"Make Snapshot from source table ${sourceTableDesc.identifier} is not " +
          s"partitioned, but a partition spec was provided.")
      }
    }
  }

  def makeExpressionWithPartitionSpec(table : CatalogTable, spec : TablePartitionSpec) : Expression = {
    spec.foldLeft[Expression] (EqualTo(Literal(1), Literal(1))) (
      (exp, kv) => And.apply(exp, EqualTo(UnresolvedAttribute(kv._1),
        Literal(StrUtils.strToAny(kv._2, table.schema.apply(kv._1).dataType))))
    )
  }

  def createTableDescByLike(sparkSession : SparkSession,
                            sourceTable : TableIdentifier,
                            targetTable: TableIdentifier,
                            isLocal: Boolean,
                            targetDir: Option[String]) : CatalogTable = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType = if (targetDir.isEmpty) CatalogTableType.MANAGED else CatalogTableType.EXTERNAL

    CatalogTable(
        identifier = targetTable,
        tableType = tblType,
        storage = sourceTableDesc.storage.copy(locationUri = targetDir.map(CatalogUtils.stringToURI(_))),
        new StructType,//schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)
  }
}

//abstract case class MakeSnapshotFromCommand(sourceTable: TableIdentifier,
//                                   partitions: java.util.List[TablePartitionSpec],
//                                   targetTable: TableIdentifier,
//                                   isLocal : Boolean,
//                                   dir: String) extends RunnableCommand {
//
//  def checkPartitionSpec(sourceTableDesc : CatalogTable) : Unit = {
//    if (sourceTableDesc.partitionColumnNames.nonEmpty) {
//      partitions.asScala.foreach(partition =>
//        if (partition.nonEmpty) {
//          partition.keySet.foreach { colName =>
//            if (!sourceTableDesc.partitionColumnNames.contains(colName)) {
//              throw new AnalysisException(s"Make Snapshot from source table $sourceTable is partitioned, " +
//                s"but the specified partition spec refers to a column that is not partitioned: " +
//                s"'$colName'")
//            }
//          }
//
//          sourceTableDesc.partitionColumnNames
//            .map(colName=>partition.contains(colName))
//            .map(b=>{b match {
//              case true => 1
//              case _ => 0
//            }}).foldLeft(1) {(l,r)=>
//            if (l<r) {
//              throw new AnalysisException(s"Make Snapshot from source table $sourceTable is partitioned, " +
//                "but the specified partition columns is not continued or not started from first partition column")
//            }
//            l
//          }
//        }
//      )
//    } else {
//      if (partitions.size() > 0) {
//        throw new AnalysisException(s"Make Snapshot from source table $sourceTable is not " +
//          s"partitioned, but a partition spec was provided.")
//      }
//    }
//  }
//}

//class HiveMakeSnapshotFromCommand(sourceTable: TableIdentifier,
//                                   partitions: java.util.List[TablePartitionSpec],
//                                   targetTable: TableIdentifier,
//                                   isLocal : Boolean,
//                                   dir: String)
//  extends MakeSnapshotFromCommand(sourceTable,partitions,targetTable,isLocal,dir) {
//
//  import org.apache.spark.sql.catalyst.parser.ParserUtils._
//
//  override def run(sparkSession: SparkSession): Seq[Row] = {
//    val logger = Logger.getLogger(classOf[MakeSnapshotFromCommand])
//
//    logger.info(s"****** HiveMakeSnapshotFromCommand : target : ${targetTable}; source : ${sourceTable}; partition : ${partitions}; isLocal : ${isLocal}; dir : ${dir}")
//
//    val catalog = sparkSession.sessionState.catalog
//
//    //TODO: catalog( 暂时不考虑自定义的 meta store )
//    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)
//    logger.info(s"sourceTableDesc : ${sourceTableDesc}; ")
//
//    checkPartitionSpec(sourceTableDesc)
//
//    //TODO : 多个 partition 分别组织 LogicPlan ，则可以在 insert into 使用静态分区
//    //TODO : 否则，where 条件查询结果 只能 通过动态分区执行 insert into
//    //TODO : 如果 source 是 view ，则没有 partition
//    val expression = partitions.asScala.foldLeft[Expression] (EqualTo(Literal(0), Literal(1))) (
//      (exp, spec) => Or.apply(exp, makeExpressionWithPartitionSpec(sourceTableDesc, spec))
//    )
//
//    val filter = UnresolvedRelation(sourceTable).optionalMap(expression)(Filter)
//
//    val localpath = isLocal match { case true => Some(dir); case _ => Some(null) }
//
//    try {
//      // create table with source table schema and maybe updated localpath
//      CreateTableLikeCommand(targetTable, targetTable, localpath, true).run(sparkSession)
//
//      // Read back the metadata of the table which was created just now.
//      val createdTableMeta = catalog.getTableMetadata(targetTable)
//
//      // TODO: here is no static partition.
//      InsertIntoHiveTable(
//        createdTableMeta,
//        Map.empty,
//        filter,
//        overwrite = true,
//        ifPartitionNotExists = false,
//        filter.output).run(sparkSession, filter)
//    } catch {
//      case NonFatal(e) =>
//        // drop the created table.
//        catalog.dropTable(tableIdentifier, ignoreIfNotExists = true, purge = false)
//        throw e
//    }
//
//    Seq.empty[Row]
//  }
//}

case class HiveSnapshotBaseBuilder(sparkSession : SparkSession) extends SnapshotBaseBuilder {
  import org.apache.spark.sql.catalyst.parser.ParserUtils._
  val logger = Logger.getLogger(classOf[HiveSnapshotBaseBuilder])

  override def build(sourceTable: TableIdentifier, partitions: java.util.List[TablePartitionSpec], targetTable: TableIdentifier, isLocal: Boolean, dir: String): LogicalPlan = {
    logger.info(s"****** HiveSnapshotCommandBuild : target : ${targetTable}; source : ${sourceTable}; partition : ${partitions}; isLocal : ${isLocal}; dir : ${dir}")
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)
    logger.info(s"sourceTableDesc : ${sourceTableDesc}; ")
    checkPartitionSpec(sourceTableDesc, partitions)

    val filter = if (partitions.size() > 0) {
      val expression = partitions.asScala.foldLeft[Expression] (EqualTo(Literal(0), Literal(1))) (
        (exp, spec) => Or.apply(exp, makeExpressionWithPartitionSpec(sourceTableDesc, spec))
      )
      UnresolvedRelation(sourceTable).optionalMap(expression)(Filter)
    } else {
      UnresolvedRelation(sourceTable)
    }

    //TODO : 多个 partition 分别组织 LogicPlan ，则可以在 insert into 使用静态分区
    //TODO : 否则，where 条件查询结果 只能 通过动态分区执行 insert into
    //TODO : 如果 source 是 view ，则没有 partition
    val localpath = isLocal match { case true => Some(dir); case _ => None }

    val targetTableDesc = createTableDescByLike(sparkSession,sourceTable,targetTable,isLocal,Some(dir))

    assert(targetTableDesc.schema.isEmpty,
      "Schema may not be specified in a Create Table As Select (CTAS) statement")

    CreateTable(targetTableDesc, SaveMode.ErrorIfExists, Some(filter))
  }
}

case class HBaseSnapshotCommand(zk : String,
                                zkPort : String,
                                hbaseTableName : String,
                                sourceTable: TableIdentifier,
                                partitions: java.util.List[TablePartitionSpec],
                                targetTable: TableIdentifier) extends RunnableCommand {
  val logger = Logger.getLogger(classOf[HBaseSnapshotCommand])

  override def run(sparkSession: SparkSession): Seq[Row] = {
    import org.apache.hadoop.hbase.TableName
    import org.apache.hadoop.hbase.client.Admin
    import org.apache.hadoop.hbase.client.Connection
    import org.apache.hadoop.hbase.client.ConnectionFactory

    val conf = new Configuration
    conf.setClassLoader(classOf[HBaseConfiguration].getClassLoader)
    conf.set("hbase.zookeeper.quorum", zk)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)

    val hbaseConf = HBaseConfiguration.create(conf)
    var connection : Connection = null
    try {
      connection = ConnectionFactory.createConnection(hbaseConf)
      val tableName = TableName.valueOf(hbaseTableName)
      val admin = connection.getAdmin
      try {
        admin.snapshot(targetTable.table, tableName)
      } catch {
        case e:Exception => logger.error(e)
      }
      admin.close()
    } catch {
      case e:Exception => logger.error(e)
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
    Seq.empty[Row]
  }
}

