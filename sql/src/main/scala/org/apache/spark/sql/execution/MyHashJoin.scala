package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BitwiseAnd, BitwiseOr, Cast, Expression, Literal, ShiftLeft, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftExistence}
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.sql.types.{IntegralType, LongType}

/**
  * Created by yilong on 2019/6/22.
  */


object MyHashJoin {
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    assert(keys.nonEmpty)
    // TODO: support BooleanType, DateType and TimestampType
    if (keys.exists(!_.dataType.isInstanceOf[IntegralType])
      || keys.map(_.dataType.defaultSize).sum > 8) {
      return keys
    }

    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      Cast(keys.head, LongType)
    } else {
      keys.head
    }
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }

  def createResultProjection(
      joinType : JoinType,
      output : Seq[Attribute],
      buildPlan : SparkPlan,
      streamedPlan : SparkPlan)(): (InternalRow) => InternalRow = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamedPlan.output ++ buildPlan.output).map(_.withNullability(true)))
  }
}
