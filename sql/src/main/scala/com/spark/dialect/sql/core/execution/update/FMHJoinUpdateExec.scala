package com.spark.dialect.sql.core.execution.update

import java.util.UUID

import com.spark.dialect.sql.core.catalyst.optimizer.UpdateOperationType
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, HashClusteredDistribution, Partitioning}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.execution.{FullMergeBroadcastHashJoinExec, MyHashJoin, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Created by yilong on 2019/7/2.
  * 使用
  * [[org.apache.spark.sql.execution.FullMergeBroadcastHashJoinExec]]
  * 执行 join 过程
  */
case class FMHJoinUpdateExec(input: Seq[Expression],
                             output: Seq[Attribute],
                             target: TableIdentifier,
                             sets: Map[Attribute,Expression],
                             condition: Option[Expression],
                             child: SparkPlan,
                             distributeKeys: Seq[Expression],
                             indexOutput: Seq[Attribute])
  extends org.apache.spark.sql.execution.UnaryExecNode {

  import com.spark.dialect.sql.core.execution.command.TableMetadataHelper._

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning
  }

  val defaultPartitionNum = 1

  //TODO:
  override def requiredChildDistribution: Seq[Distribution] = {
    HashClusteredDistribution(distributeKeys, Some(defaultPartitionNum)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val _log = Logger.getLogger("FullMergeHashJoinUpdateExec")

    _log.info(s"************ doExecute:: input : ${input}; output : ${output}; child: ${child} ")

    val spark = sqlContext.sparkSession
    val allOutputSchema = StructType(child.output.map(x => new StructField(x.name, x.dataType)))
    val indexOutputSchema = StructType(indexOutput.map(x => new StructField(x.name, x.dataType)))

    val allOutputReferences = child.output.map(x =>
      BindReferences.bindReference[Expression](x, child.output)
    )

    val indexOutputBindReferences = indexOutput.map(x =>
      BindReferences.bindReference[Expression](x, indexOutput)
    )

    val childRDD = child.execute().mapPartitions(rowit=>rowit.map(row=>{
      val nrow = UnsafeProjection.create(indexOutputBindReferences.toSeq)(row)
      CatalystTypeConverters.convertToScala(nrow, indexOutputSchema).asInstanceOf[Row]
    }))

    child.execute().collect().foreach(row => {
      val nrow = UnsafeProjection.create(allOutputReferences.toSeq)(row)
      val mrow = CatalystTypeConverters.convertToScala(nrow, allOutputSchema).asInstanceOf[Row]
      _log.info(s" row in join rdd : ${mrow} : ${allOutputSchema}")
    })

    childRDD.collect().foreach(row => {
      _log.info(s" row in child rdd : ${row} : ${indexOutputSchema}")
    })

    val targetTable = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(target)
    //val tempTarget = target.copy(table = target.table+"_"+UUID.randomUUID().toString.replace("-","_"),database = target.database)
    //val tempTargetTable = createTableDescByLike(spark, target, tempTarget, false, None)
    //val ct = CreateTable(tempTargetTable, SaveMode.ErrorIfExists, None)
    //val createExec = new QueryExecution(spark,ct).executedPlan
    //createExec.executeCollect()

    HiveUpdateAction(spark, targetTable, indexOutputSchema).save(childRDD, UpdateOperationType.WhloeUpdate)

    child.execute().mapPartitions{rowIt => rowIt.map(row => row)}
    //Seq.empty[InternalRow].toDS().rdd
  }
}
