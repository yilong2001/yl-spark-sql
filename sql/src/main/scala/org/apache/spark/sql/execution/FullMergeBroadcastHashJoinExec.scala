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

import com.spark.dialect.sql.core.catalyst.rules.{ExpressionTransformHelper, ReplaceAttributeExp, ReplaceAttributeExpWithExprId}
import com.spark.dialect.sql.core.execution.update.{UpdateConstantReplacer, UpdatePerformerIterator}
import com.spark.dialect.sql.util.{ExpressionEvalHelper, StrUtils}
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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.unsafe.map.BytesToBytesMap

import scala.collection.mutable

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 *
  * Full Merge Broadcast Hash Join 与 BroadcastHashJoin 的区别:
  * 1)  Right Plan (待 update 记录)  不包含 Condtition 条件，也不做 push down predicts
  * 2)  输出记录是基于 right plan 的全量数据
  * 3)  全量 right plan 与 left plan 根据 condition 做 merge
  * 4)  不匹配的记录，保持 right value ；匹配的记录，使用 left value 进行更新
 */

case class FullMergeBroadcastHashJoinExec(
    buildPlanKeys: Seq[Expression],
    indexPlanKeys: Seq[Expression],
    output: Seq[Attribute],
    condition: Option[Expression],
    buildSparkPlan: SparkPlan,
    indexSparkPlan: SparkPlan,
    sets: Map[Attribute, Expression])
  extends BinaryExecNode /* with CodegenSupport */ /* with HashJoin with CodegenSupport */ {

  override def left: SparkPlan = indexSparkPlan
  override def right: SparkPlan = buildSparkPlan

  //TODO: set by user or calc with data counts
  override def outputPartitioning: Partitioning = {
    indexSparkPlan.outputPartitioning
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val bindReferences = MyHashJoin.rewriteKeyExpr(buildPlanKeys).map(BindReferences.bindReference(_, buildSparkPlan.output))

    val mode = HashedRelationBroadcastMode(bindReferences)

    UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val sparkSession = sqlContext.sparkSession

    val broadcastRelation = buildSparkPlan.executeBroadcast[HashedRelation]()

    val buildPlanOutputSchema: StructType = StructType(buildSparkPlan.output.map(a => new StructField(a.name, a.dataType)))

    val allOutputAttributes = mutable.ArrayBuffer[Attribute]()
    val allOutStructFields = new mutable.ArrayBuffer[StructField]()
    (indexSparkPlan.output ++ buildSparkPlan.output).foreach(x => {
      allOutputAttributes.append(x)
      allOutStructFields.append(new StructField(x.name, x.dataType))
    })
    val allOutputAttrWithNull = allOutputAttributes.map(_.withNullability(true))

    val localCondition = condition.get

    val allOutputBindReferences = allOutputAttrWithNull.map(x =>
      BindReferences.bindReference[Expression](x, allOutputAttrWithNull)
    )

    val allOutputSchema: StructType = StructType(allOutStructFields)
    val upSets = new mutable.HashMap[Attribute, Expression]()
    sets.foreach(tp => upSets.put(tp._1, tp._2))

    val indexOutputAttributes = new mutable.ArrayBuffer[Attribute]()
    indexSparkPlan.output.foreach(x => indexOutputAttributes.append(x))

    val indexOutputAttributesWithNull = indexOutputAttributes.map(_.withNullability(true))
    val indexOutputBindReferences = indexOutputAttributesWithNull.map(x => {
      BindReferences.bindReference[Expression](x, indexOutputAttributesWithNull)
    })

    val indexKeyBindReferences = MyHashJoin.rewriteKeyExpr(indexPlanKeys).map(BindReferences.bindReference(_, indexSparkPlan.output))

    indexSparkPlan.execute().mapPartitions { indexIter =>
      val hashed = broadcastRelation.value.asReadOnlyCopy()
      def indexSideKeyGeneratorFunc() = UnsafeProjection.create(indexKeyBindReferences)

      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)

      val nullBuildRow = new SpecificInternalRow(buildPlanOutputSchema)

      val matchConditionFunc = if (localCondition != null) {
        GeneratePredicate.generate(localCondition, allOutputAttributes).eval _
      } else {
        (r: InternalRow) => true
      }

      val joinRow = new JoinedRow
      indexIter.flatMap { indexRow =>
        joinRow.withLeft(indexRow)//joinRow.withRight(indexRow)
        val matches : Iterator[InternalRow] = hashed.get(indexSideKeyGeneratorFunc()(indexRow))
        if (matches != null) {
          matches.map(joinRow.withRight(_)/*.withLeft(_)*/).map(jr => {
            val newrow = new SpecificInternalRow(allOutputSchema)
            val valueMap = new mutable.HashMap[String,Any]()

            allOutputSchema.foreach(sf => {
              val index = allOutputSchema.indexOf(sf)
              valueMap.put(sf.name, jr.get(index, sf.dataType))
              newrow.update(index, jr.get(index, sf.dataType))
            })

            if (matchConditionFunc(jr)) {
              //TODO: replace set field with right value
              upSets.foreach(tp => {
                val exp = ReplaceAttributeExp(valueMap.toMap, allOutputSchema, true)(tp._2)
                val sQLConf = new SQLConf()

                //var expVal = ExpressionTransformHelper.resolveExpression(sparkSession, sQLConf)(exp)
                //val lastVal1 = ExpressionEvalHelper.evaluateWithGeneratedMutableProjection(expVal)
                val lastVal = ExpressionEvalHelper.evaluateWithGeneratedMutableProjection(exp, jr)
                valueMap.put(StrUtils.splitString(tp._1.name, ".").last, lastVal)
              })

              valueMap.foreach(x => {
                val sf = allOutputSchema.apply(x._1)
                val index = allOutputSchema.indexOf(sf)
                newrow.update(index, valueMap.get(x._1).get)
              })
            } else {
            }

            val unsaferow = UnsafeProjection.create(allOutputBindReferences.toSeq)(newrow)

            unsaferow
          })
        } else {
          joinRow.withRight(nullBuildRow)
          val unsaferow = UnsafeProjection.create(allOutputBindReferences.toSeq)(joinRow)
          Seq(unsaferow)
        }
      }
    }

  }
}
