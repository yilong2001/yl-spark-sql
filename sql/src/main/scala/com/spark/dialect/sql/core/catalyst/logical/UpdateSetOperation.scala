package com.spark.dialect.sql.core.catalyst.logical

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ScriptInputOutputSchema, UnaryNode}

/**
  * Created by yilong on 2019/6/8.
  */

case class UpdateSetOperation(input: Seq[Expression],
                child: LogicalPlan,
                target: TableIdentifier,
                sets:Map[Attribute,Expression],
                originCondition: Option[Expression],
                renamed:Boolean) extends UnaryNode {
  override def output: Seq[Attribute] = child.output
  override def references: AttributeSet = AttributeSet(input.flatMap(_.references))
}
