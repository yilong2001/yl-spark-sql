package com.spark.dialect.sql.core.catalyst.rules

import com.spark.dialect.sql.core.catalyst.logical.UpdateSetOperation
import com.spark.dialect.sql.util.StrUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec

import scala.collection.mutable

/**
  * Created by yilong on 2019/6/9.
  */
class UpdateSetOperationRules(spark:SparkSession) extends Rule[LogicalPlan] {
  val logger = Logger.getLogger(classOf[UpdateSetOperationRules])

  def renameAttribute(attr : org.apache.spark.sql.catalyst.expressions.Attribute) : String = {
    attr.name + "__t"
  }

  def renameAttributeWithExpression(attrMap : Map[org.apache.spark.sql.catalyst.expressions.Attribute, Alias],
                                    exp : Expression) : Expression = {
    exp transformUp {
      case attribute : org.apache.spark.sql.catalyst.expressions.Attribute => {
        val ra = attrMap.get(attribute)
        if (ra != None && ra != null) {
          attribute.newInstance().withExprId(ra.get.exprId).withName(ra.get.name).withQualifier(attribute.qualifier)
        } else {
          attribute
        }
      }
      case ua : UnresolvedAttribute => {
        val ra = attrMap.get(ua)
        if (ra != None && ra != null) {
          ua.newInstance().withExprId(ra.get.exprId).withName(ra.get.name).withQualifier(ua.qualifier)
        } else {
          ua
        }
      }
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case u : UpdateSetOperation if !u.renamed => {
        withOrigin(u.origin) {
          logger.info(u)
          val  join = u.child.asInstanceOf[Join]

          val renamedRightAttrMap = new mutable.HashMap[org.apache.spark.sql.catalyst.expressions.Attribute, Alias]()
          val renamedRightOutput = join.right.output.map(att => {
            logger.info(att)

            val rattr = Alias(att, renameAttribute(att))()
            //TODO: for hive modification
            //val rattr = att.newInstance().withName(renameAttribute(att))//Alias(att, renameAttribute(att))()
            renamedRightAttrMap.put(att, rattr)

            rattr
          })

          val leftAttrMap = new mutable.HashMap[String, org.apache.spark.sql.catalyst.expressions.Attribute]()
          join.left.output.foreach(att => {
            leftAttrMap.put(att.name, att)
          })

          val leftOutput = join.left.output

          val leftRelationName = join.left match {
            case alias : SubqueryAlias => alias.name.identifier
            case rl : LogicalRelation => rl.relation.schema.names.mkString(".")
            case _ => throw new IllegalArgumentException(s"${join.right} is error!")
          }

          val renamedRight = join.right match {
            case alias : SubqueryAlias => {
              SubqueryAlias(alias.name.identifier, Project(renamedRightOutput, join.right))
            }
            case _ => {
              Project(renamedRightOutput, join.right)
            }
            case _ => throw new IllegalArgumentException(s"${u} is error!")
          }

          val rightRelationName = join.right match {
            case alias : SubqueryAlias => alias.name.identifier
            case rl : LogicalRelation => rl.relation.schema.names.mkString(".")
            case _ => throw new IllegalArgumentException(s"${join.right} is error!")
          }

          def replaceRightAttribute(x : org.apache.spark.sql.catalyst.expressions.Attribute) : Expression = {
            if (!x.name.contains(rightRelationName) && !x.qualifier.mkString(".").equals(rightRelationName)) {
              x
            } else {
              val fn = StrUtils.splitString(x.name, ".").last
              val ax = renamedRightAttrMap.keySet.find(a => {if (fn.equals(a.name)) true else false})
              if (ax != None) {
                val alias = renamedRightAttrMap.get(ax.get).get
                alias
              }
              else {
                throw new IllegalArgumentException(s"${x} , has error attribute!!!")
              }
            }
          }

          def replaceLeftAttribute(x : org.apache.spark.sql.catalyst.expressions.Attribute) :
          org.apache.spark.sql.catalyst.expressions.Attribute = {
            if (!x.name.contains(leftRelationName) && !x.qualifier.mkString(".").equals(leftRelationName)) {
              x
            } else {
              val fn = StrUtils.splitString(x.name, ".").last
              leftAttrMap.get(fn).get
            }
          }

          //val renamedExp = renameAttributeWithExpression(renamedRightAttrMap.toMap, join.condition.get)
          val renamedExp = join.condition.get transform {
            case ua: UnresolvedAttribute => {
              if (ua.name.contains(leftRelationName)) {
                replaceLeftAttribute(ua).asInstanceOf[Expression]
              } else if (ua.name.contains(rightRelationName)) {
                val rua = replaceRightAttribute(ua)
                rua match {
                  case alias : Alias => alias.toAttribute.withName(alias.name).withExprId(alias.exprId)
                  case other => other
                }
              } else {
                ua
              }
            }
            case af: AttributeReference => {
              val qualifer = af.qualifier.mkString(".")
              if (qualifer.equals(rightRelationName)) {
                val rua = replaceRightAttribute(af)
                rua match {
                  case alias : Alias => alias.toAttribute.withName(alias.name).withExprId(alias.exprId)
                  case other => other
                }
              } else {
                af
              }
            }
          }

          val renamedSets = u.sets.map(tp => {
            (replaceLeftAttribute(tp._1), tp._2.transform({
              case ua : UnresolvedAttribute => {
                val rua = replaceRightAttribute(ua)
                rua match {
                  case alias : Alias => alias.toAttribute.withName(alias.name).withExprId(alias.exprId)
                  case other => other
                }
              }
              case af : AttributeReference => {
                replaceRightAttribute(af)
              }
              case other => other
            }))
          })

          //TODO:
          val newjoin = Join(join.left, renamedRight, join.joinType, Some(renamedExp))
          val uo = UpdateSetOperation(newjoin.output,
            newjoin,
            u.target, renamedSets, Some(renamedExp)/*join.condition*/, true)

          logger.info(uo)

          uo
        }
      }
      case _ => plan
    }
  }
}
