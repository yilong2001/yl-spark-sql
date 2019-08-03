package com.spark.dialect.sql.core.execution.update

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

/**
  * Created by yilong on 2019/6/8.
  * 使用
  * [[org.apache.spark.sql.execution.joins.BroadcastHashJoinExec]]
  * 执行 join 过程
  */


case class BHJoinUpdateExec(input: Seq[Expression],
                            output: Seq[Attribute],
                            target: TableIdentifier,
                            sets: Map[Attribute,Expression],
                            condition: Option[Expression],
                            child: SparkPlan,
                            distributeKeys: Seq[Expression])
  extends org.apache.spark.sql.execution.UnaryExecNode
  /* with CodegenSupport */ {
  final val logger = Logger.getLogger("UpdateOperationSimpleExec")

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning
  }

  //TODO:
  override def requiredChildDistribution: Seq[Distribution] = {
    HashClusteredDistribution(distributeKeys, Some(defaultPartitionNum)) :: Nil
  }

  val defaultPartitionNum = 2


  override protected def doExecute(): RDD[InternalRow] = {
    logger.info(s"************ doExecute:: input : ${input}; output : ${output}; child: ${child} ")

    val spark = sqlContext.sparkSession
    val outputAttr = child.output
    val outputSchema = StructType(outputAttr.map(x => new StructField(x.name, x.dataType)))
    val destTable = target.copy()
    val mysets = sets
    child.execute().mapPartitions(rowIt => {
      UpdatePerformerIterator(spark, destTable, rowIt, outputSchema, outputAttr, mysets)
    })
  }
}
