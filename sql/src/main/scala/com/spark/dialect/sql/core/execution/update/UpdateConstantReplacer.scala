package com.spark.dialect.sql.core.execution.update

import com.spark.dialect.sql.core.catalyst.rules.ReplaceAttributeExp
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, ExprId, Expression, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types.StructType

/**
  * Created by yilong on 2019/6/29.
  */
object UpdateConstantReplacer {
  def updateIndexPlanCondition(candidateKeys: Seq[Expression], outputSchema:StructType, valueMap: Map[String, Any], orgPlan: LogicalPlan): LogicalPlan = {
    val keyExpIds = candidateKeys.map(exp => {
      exp match {
        case attr : Attribute => attr.exprId
        case attrRef : AttributeReference => attrRef.exprId
        case _ => ExprId(-1)
      }
    })

    def findExprId(dest: ExprId) : Boolean = {
      keyExpIds.filter(x => dest.equals(x)).nonEmpty
    }

    val reOrgPlan = orgPlan transform {
      case f : Filter => {
        val ref = f transformExpressions {
          case attr : Attribute  => {
            if (findExprId(attr.exprId)) {
              ReplaceAttributeExp(valueMap, outputSchema, true)(attr)
            } else { attr }
          }
          case attrR : AttributeReference => {
            if (findExprId(attrR.exprId)) {
              ReplaceAttributeExp(valueMap, outputSchema, true)(attrR)
            } else {
              attrR
            }
          }
        }

        ref
      }
    }

    reOrgPlan
  }
}
