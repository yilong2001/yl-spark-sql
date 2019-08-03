/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import com.spark.dialect.sql.core.catalyst.rules.{ReplaceAttributeExp, ReplaceAttributeExpWithExprId}
import com.spark.dialect.sql.core.execution.update.{UpdateConstantReplacer, UpdatePerformerIterator}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.mutable

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class UpdatedIndexSparkPlan(val sparkSession : SparkSession,
                val structType : StructType,
                val valueMap:Map[String,Any],
                val child: SparkPlan)
  extends UnaryExecNode with CodegenSupport {

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.execute() :: Nil
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def output: Seq[Attribute] = child.output
}

case class IndexedHashJoinExec(
    buildPlanKeys: Seq[Expression],
    indexPlanKeys: Seq[Expression],
    output: Seq[Attribute],
    condition: Option[Expression],
    buildSparkPlan: SparkPlan,
    indexSparkPlan: SparkPlan,
    indexPlan: LogicalPlan)
  extends UnaryExecNode /* with CodegenSupport */ /* with HashJoin with CodegenSupport */ {

  override def child: SparkPlan = buildSparkPlan

  //TODO: set by user or calc with data counts
  val defaultPartitionNum = 2

  override def outputPartitioning: Partitioning = {
    indexSparkPlan.outputPartitioning
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    HashClusteredDistribution(buildPlanKeys, Some(defaultPartitionNum)) :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val _log = Logger.getLogger("IndexedHashJoinExec")

    val sparkSession = sqlContext.sparkSession

    val buildPlanOutputSchema: StructType = StructType(buildSparkPlan.output.map(a => new StructField(a.name, a.dataType)))

    val allAttr = mutable.ArrayBuffer[Attribute]()
    val structFieldArr = new mutable.ArrayBuffer[StructField]()
    (indexSparkPlan.output ++ buildSparkPlan.output).foreach(x => {
      allAttr.append(x)
      structFieldArr.append(new StructField(x.name, x.dataType))
    })
    val allAttrWithNull = allAttr.map(_.withNullability(true))

    val buildBindReferences = new mutable.HashMap[String, BoundReference]()
    buildSparkPlan.output.foreach(x => {
      val bind = BindReferences.bindReference[Expression](x, allAttrWithNull)
      buildBindReferences.put(x.name, bind.asInstanceOf[BoundReference])
    }
    )

    val buildRDD = buildSparkPlan.executeToIterator()
    val indexedPlans = new mutable.ArrayBuffer[SparkPlan]()

    val allBindAttributes = allAttrWithNull.map(x =>
      BindReferences.bindReference[Expression](x, allAttrWithNull)
    )

    val childOutputSchema: StructType = StructType(structFieldArr)
    val mySets = new mutable.HashMap[Attribute, Expression]()

    _log.error(s"_______________________buildSparkPlan:  ${buildSparkPlan}")

    buildRDD.map {
      lrow => {
        val valueMap = new mutable.HashMap[String, Any]()

        buildPlanOutputSchema.foreach(sf => {
          val dt = buildBindReferences.get(sf.name).get
          valueMap.put(sf.name, lrow.get(dt.ordinal, dt.dataType));
        })

        val queryExec = new QueryExecution(sparkSession,
          UpdateConstantReplacer.updateIndexPlanCondition(buildPlanKeys, buildPlanOutputSchema, valueMap.toMap, indexPlan)).executedPlan

        _log.error(s"_______________________queryExec:  ${queryExec}")

        queryExec.execute().mapPartitions(rrowIt => {
          rrowIt.map(rrow => {
            val newrow = new JoinedRow(rrow, lrow)
            val unsafeProjection = UnsafeProjection.create(allBindAttributes)
            unsafeProjection(newrow).asInstanceOf[InternalRow]
          })
        })
      }
    }.reduce(_.union(_))
  }
}
