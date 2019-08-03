package com.spark.dialect.sql.core.catalyst.rules

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ComplexTypeMergingExpression, Concat, Expression, ListQuery, NamedExpression, TimeZoneAwareExpression}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.ImplicitTypeCasts
import org.apache.spark.sql.catalyst.analysis.{TypeCoercionRule, UnresolvedFunction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
  * Created by yilong on 2019/5/27.
  */


object ExpressionTransformHelper {
  def resolveExpression(spark:SparkSession, conf:SQLConf)(exp:Expression) : Expression = {
    var texp = exp
    getRules(spark,conf).foreach(
      r => {
        texp = r.apply(texp)
      }
    )

    texp
  }

  def getRules(spark:SparkSession, conf:SQLConf) : List[ExpressionRule] = {
    List(
      ResolveFunctionExp(spark, conf),
      ResolveTimeZoneExp(conf),
      CastConcatExp(conf)
    )
  }

//  def transform(exp:Expression, rule: PartialFunction[Expression, Expression]): Expression = {
//    exp.transformDown(rule)
//  }
//
//  def transformUp(exp:Expression, rule: PartialFunction[Expression, Expression]): Expression = {
//    exp.transformUp(rule)
//  }
}


abstract class ExpressionRule extends org.apache.spark.sql.catalyst.rules.Rule[Expression] {
  def transform(exp:Expression, rule: PartialFunction[Expression, Expression]): Expression = {
    exp.transformDown(rule)
  }

  def transformUp(exp:Expression, rule: PartialFunction[Expression, Expression]): Expression = {
    exp.transformUp(rule)
  }
}

case class ResolveTimeZoneExp(conf : SQLConf) extends ExpressionRule {
  private val transformTimeZoneExprs: PartialFunction[Expression, Expression] = {
    case e: TimeZoneAwareExpression if e.timeZoneId.isEmpty =>
      e.withTimeZone(conf.sessionLocalTimeZone)
  }

  override def apply(exp: Expression): Expression = {
    //transform(exp, transformTimeZoneExprs)
    exp transform transformTimeZoneExprs
  }
}

case class ResolveFunctionExp(spark:SparkSession, conf : SQLConf) extends ExpressionRule {
  val logger = Logger.getLogger(classOf[ResolveFunctionExp])

  private val transformUnresolvedFunctionExprs: PartialFunction[Expression, Expression] = {
    case uf @ UnresolvedFunction(name, children, isDistinct) => {
      try {
        val rf = spark.sessionState.catalog.lookupFunction(name, children)
        logger.info(rf)

        rf
      } catch {
        case e : Exception => {
          logger.error(e.getMessage, e)
          uf
        }
      }
    }
    case other => other
  }

  override def apply(exp: Expression): Expression = {
    //transform(exp, transformUnresolvedFunctionExprs)
    exp transform transformUnresolvedFunctionExprs
  }
}

trait ExpTypeCoercionRule extends ExpressionRule {
  private val transformTypeCoercionExprs: PartialFunction[Expression, Expression] = {
    case e => coerceTypes(e)
  }

  /**
    * Applies any changes to [[AttributeReference]] data types that are made by the transform method
    * to instances higher in the query tree.
    */
  def apply(exp: Expression): Expression = {
    coerceTypes(exp)
    //ExpressionTransformHelper.transform(exp, transformTypeCoercionExprs)
//    val newPlan = coerceTypes(exp)
//    if (exp.fastEquals(newPlan)) {
//      exp
//    } else {
//      newPlan//propagateTypes(newPlan)
//    }
  }

  protected def coerceTypes(exp: Expression): Expression

//  private def propagateTypes(exp: Expression): Expression = exp match {
//    // No propagation required for leaf nodes.
//    case q: Expression if q.children.isEmpty => q
//
//    // Don't propagate types from unresolved children.
//    case q: Expression if !q.childrenResolved => q
//
//    case q: Expression =>
//      val inputMap = q.inputSet.toSeq.map(a => (a.exprId, a)).toMap
//      q transformExpressions {
//        case a: AttributeReference =>
//          inputMap.get(a.exprId) match {
//            // This can happen when an Attribute reference is born in a non-leaf node, for
//            // example due to a call to an external script like in the Transform operator.
//            // TODO: Perhaps those should actually be aliases?
//            case None => a
//            // Leave the same if the dataTypes match.
//            case Some(newType) if a.dataType == newType.dataType => a
//            case Some(newType) =>
//              logDebug(
//                s"Promoting $a from ${a.dataType} to ${newType.dataType} in ${q.simpleString}")
//              newType
//          }
//      }
//  }
}

case class CastConcatExp(conf: SQLConf) extends ExpTypeCoercionRule {
  override protected def coerceTypes(exp: Expression): Expression = {
    exp match { case p =>
      p transformUp {
        // Skip nodes if unresolved or empty children
        case c @ Concat(children) if /*!c.childrenResolved ||*/ children.isEmpty => c
        case c @ Concat(children) if conf.concatBinaryAsString ||
          !children.map(_.dataType).forall(_ == BinaryType) =>
          val newChildren = c.children.map { e =>
            ImplicitTypeCasts.implicitCast(e, StringType).getOrElse(e)
          }
          c.copy(children = newChildren)
      }
    }
  }
}
