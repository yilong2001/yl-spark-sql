package com.spark.dialect.sql.core.catalyst.rules

import java.util

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
  * Created by yilong on 2019/5/29.
  */
case class ReplaceAttributeExpWithExprId(constantMap:Map[ExprId,Any], schema:StructType, skipTableId:Boolean) extends ExpressionRule {
  override def apply(exp: Expression): Expression = {
    exp transform {
      case ua @ UnresolvedAttribute(nameParts) => {
        val attrv = constantMap.get(ua.exprId)
        if (attrv == null || attrv == None) {
          //throw new IllegalArgumentException(s"${exp} is not support!")
          logError(s"${exp} is not support!")
          ua
        } else {
          val fd = if (skipTableId) nameParts.last else nameParts.mkString(".")
          val lt = org.apache.spark.sql.catalyst.expressions.Literal.create(attrv.get, schema.apply(fd).dataType)
          lt
        }
      }
      case ar @ AttributeReference(name,dataType,nullable,metadata) => {
        val attrv = constantMap.get(ar.exprId)
        if (attrv == null || attrv == None) {
          //throw new IllegalArgumentException(s"${exp} is not support!")
          logError(s"${exp} is not support!")
          ar
        } else {
          val lt = org.apache.spark.sql.catalyst.expressions.Literal.create(attrv.get, schema.apply(name).dataType)
          lt
        }
      }
    }
  }
}

case class ReplaceAttributeExp(constantMap:Map[String,Any], schema:StructType, skipTableId:Boolean) extends ExpressionRule {
  override def apply(exp: Expression): Expression = {
    exp transform {
      case alias @ Alias(child, name) => {
        val childv = constantMap.get(name)
        if (childv == null || childv == None) {
          alias
        } else {
          val lt = org.apache.spark.sql.catalyst.expressions.Literal.create(childv.get, schema.apply(name).dataType)
          lt
        }
      }
      case ua @ UnresolvedAttribute(nameParts) => {
        val fd = if (skipTableId) nameParts.last else nameParts.mkString(".")
        val attrv = constantMap.get(fd)
        if (attrv == null || attrv == None) {
          //throw new IllegalArgumentException(s"${exp} is not support!")
          logError(s"${exp} is not support!")
          ua
        } else {
          val lt = org.apache.spark.sql.catalyst.expressions.Literal.create(attrv.get, schema.apply(fd).dataType)
          lt
        }
      }
      case ar @ AttributeReference(name,dataType,nullable,metadata) => {
        val attrv = constantMap.get(name)

        if (attrv == null || attrv == None) {
          //throw new IllegalArgumentException(s"${exp} is not support!")
          logError(s"${exp} is not support!")
          ar
        } else {
          val lt = org.apache.spark.sql.catalyst.expressions.Literal.create(attrv.get, schema.apply(name).dataType)
          lt
        }
      }
    }
  }
}


case class ResolveAttributeExp(schema:StructType) extends ExpressionRule {
  override def apply(exp: Expression): Expression = {
    exp transform {
      case UnresolvedAttribute(nameParts) => {
        val fd = nameParts.mkString(".")
        AttributeReference(fd, schema.apply(fd).dataType)(NamedExpression.newExprId, Seq()) //Option("f")
      }
    }
  }
}
