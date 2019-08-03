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
  * [[IndexedHashJoinExec]]
  * 执行 join 过程
  */


case class IndexedHashJoinUpdateExec(input: Seq[Expression],
                                     output: Seq[Attribute],
                                     target: TableIdentifier,
                                     sets: Map[Attribute,Expression],
                                     child: SparkPlan,
                                     distributionKey: Seq[Expression])
  extends org.apache.spark.sql.execution.UnaryExecNode
  /* with CodegenSupport */ {
  final val logger = Logger.getLogger("IndexedHashJoinUpdateExec")

  override def outputPartitioning: Partitioning = child.outputPartitioning

  //TODO:
  override def requiredChildDistribution: Seq[Distribution] = {
    ClusteredDistribution(distributionKey) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logger.info(s"************ doExecute:: input : ${input}; output : ${output}; child: ${child} ")

    val sparkSession = sqlContext.sparkSession
    val allAttr = mutable.ArrayBuffer[Attribute]()
    val structFieldArr = new mutable.ArrayBuffer[StructField]()
    (child.output).foreach(x => {
      allAttr.append(x)
      structFieldArr.append(new StructField(x.name, x.dataType))
    })
    val allAttrWithNull = allAttr.map(_.withNullability(true))
    val childOutputSchema: StructType = StructType(structFieldArr)
    val mySets = new mutable.HashMap[Attribute, Expression]()
    sets.foreach(tp => mySets.put(tp._1, tp._2))

    val destTable = target.copy()

    child.execute().mapPartitions(rowIt => {
      UpdatePerformerIterator(sparkSession, destTable, rowIt, childOutputSchema, allAttrWithNull, mySets.toMap)
    })
  }
}
