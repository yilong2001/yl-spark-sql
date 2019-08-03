package com.spark.dialect.sql.core.catalyst.logical

import com.spark.dialect.sql.core.catalyst.logical.TrainAlgorithmType.TrainAlgorithmType
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}

/**
  * Created by yilong on 2019/7/28.
  */

object TrainAlgorithmType extends Enumeration {
  type TrainAlgorithmType = Value
  val LR, XGB = Value
}


case class TrainModelIntoPath (input: Seq[Expression],
                          child: LogicalPlan,
                          target: String,
                          algorithmType: TrainAlgorithmType) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def references: AttributeSet = AttributeSet(input.flatMap(_.references))
}

