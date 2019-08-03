package com.spark.dialect.sql.core.catalyst.optimizer

import javax.management.relation.Relation

import com.spark.dialect.sql.core.catalyst.logical.UpdateSetOperation
import com.spark.dialect.sql.core.catalyst.optimizer.UpdateOperationType.UpdateOperationType
import com.spark.dialect.sql.core.catalyst.rules.{ExpressionTransformHelper, ReplaceAttributeExp, UpdateSetOperationRules}
import com.spark.dialect.sql.core.execution.update.{BHJoinUpdateExec, FMHJoinUpdateExec, IndexedHashJoinUpdateExec, OneStageUpdateExec}
import com.spark.dialect.sql.util.MetaConstants
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo, ExprId, Expression, Literal, Not, Or, RowOrdering}
import org.apache.spark.sql.execution.{FullMergeBroadcastHashJoinExec, IndexedHashJoinExec, SparkPlan, joins}
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, logical}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.execution.joins.{BroadcastNestedLoopJoinExec, BuildLeft, BuildRight}
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by yilong on 2019/6/9.
  */
object UpdateOperationType extends Enumeration {
  type UpdateOperationType = Value
  val IndexedUpdate, WhloeUpdate = Value
}

object UpdateSetOperationStrategy {
  final val logger = Logger.getLogger(classOf[UpdateSetOperationStrategy])
  def info(obj:AnyRef) : Unit = {
    logger.info(obj)
  }

  def findOriginalAttributeByExprId(exprIds: Seq[Expression], dest: ExprId) : Expression = {
    val out = exprIds.filter(x => {
      val matched = x match {
        case alias @ Alias(child, name) => dest.equals(alias.exprId)
        case attr : Attribute => dest.equals(attr.exprId)
        case _ => false
      }
      matched
    })

    if (out.isEmpty) null else {
      out.apply(0) match {
        case alias @ Alias(child, name) => child
        case _ => out.apply(0)
      }
    }
  }

  def reverseAttributeByAlias(orgExp: Expression, exprIds: Seq[Expression]) : Expression = {
    val down = orgExp transform {
      case attr : AttributeReference => {
        val matchedAttr = findOriginalAttributeByExprId(exprIds, attr.exprId)
        if (matchedAttr == null) {
          attr
        } else {
          matchedAttr
        }
      }
    }

    down
  }

  def pushdownPredictOnRight(join: Join, condition: Expression): LogicalPlan = {
    join.right match {
      case alias @ SubqueryAlias(id, pj @ Project(projectList, child)) => {
        val downCond = reverseAttributeByAlias(condition, projectList)
        SubqueryAlias(id, Project(projectList, Filter((downCond), child)))
      }
      case pj @ Project(projectList, child) => {
        val downCond = reverseAttributeByAlias(condition,projectList)
        Project(projectList, Filter((downCond), child))
      }
      case _ => throw new IllegalArgumentException(s"condition : ${condition} is error!")
    }
  }

  def pushdwonPredictOnLeft(join: Join, condition: Expression): LogicalPlan = {
    join.left match {
      case alias @ SubqueryAlias(id, pj @ Project(projectList, child)) => {
        SubqueryAlias(id, Project(projectList, Filter(condition, child)))
      }
      case alias @ SubqueryAlias(id, child) => {
        SubqueryAlias(id, Filter(condition, child))
      }
      case pj @ Project(projectList, child) => {
        Project(projectList, Filter(condition, child))
      }
      case filter @ Filter(cond, child) => {
        Filter(And(condition, cond), child)
      }
      case _ => throw new IllegalArgumentException(s"condition : ${condition} is error!")
    }
  }

  def makeUpdateOperationType(targetTableDesc: CatalogTable, indexKeys: Seq[Expression]) : UpdateOperationType = {
    val storageType = targetTableDesc.properties.get(MetaConstants.META_DATA_STORAGE_TYPE)

    //TODO: 只处理简单的情况，如果 update set 的 where 条件是组合条件（例如： or ... ）
    //TODO: or 跟随的条件不包含 rowkey 的 first field，则不能正确处理

    storageType match {
      case None => UpdateOperationType.WhloeUpdate
      case Some(MetaConstants.META_DATA_STORAGE_TYPE_HBASE) => {
        val rowkeyFieldsFirst = targetTableDesc.properties.get(MetaConstants.META_DATA_HBASE_ROWKEY_FIELDS_FIRST)
        if (rowkeyFieldsFirst != null && rowkeyFieldsFirst.nonEmpty) {
          //TODO: 需要校验 rowkey field 包含在当前 condition 表达式中
          UpdateOperationType.IndexedUpdate
        } else {
          UpdateOperationType.WhloeUpdate
        }
      }
      case Some(MetaConstants.META_DATA_STORAGE_TYPE_ES) |
           Some(MetaConstants.META_DATA_STORAGE_TYPE_SOLR) => {
        //TODO: 需要校验 condition 中的字段是索引字段 （默认符合）
        UpdateOperationType.IndexedUpdate
      }
      case _ => UpdateOperationType.WhloeUpdate
    }
  }

  def resolveJoinLogicalPlan(plan : LogicalPlan) : (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan) = {
    plan match {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) => {
        logger.info(s"BroadcastHashJoinExec : ${joinType}, ${leftKeys}, ${rightKeys}, ${condition}, ${left}, ${right}")
        (joinType, leftKeys, rightKeys, condition, left, right)
      }
      case _ => {
        logger.info(s"update condition : ${plan}, no equal expression")
        (null, Seq.empty, Seq.empty, null, null, null)
      }
    }
  }

}

class UpdateSetOperationStrategy(spark : SparkSession) extends sql.Strategy {
  import UpdateSetOperationStrategy._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case u : UpdateSetOperation => {
        withOrigin(u.origin) {
          //TODO: 本质上，这是一个 right inner join 操作，因为 被更新的 table 被放在了 join 的 left
          info(s"UpdateSetOperationStrategy : UpdateSetOperation : ${u}")

          val catalog = spark.sessionState.catalog
          val targetTableDesc = catalog.getTempViewOrPermanentTableMetadata(u.target)

          val join = (u.child.asInstanceOf[Join])
          val jout = join.output
          def findAttributeReference(att:UnresolvedAttribute):Attribute = {
            val filter = jout.find(p => {
              p.name.equals(att.name)
            })
            if (filter.isEmpty) att else filter.get
          }
          val newSets = u.sets.map( tp => {
            val n2 = tp._2 transform {
              case af : AttributeReference => af
              case att : UnresolvedAttribute =>  {
                val af = findAttributeReference(att)
                af
              }
            }
            (tp._1 -> n2)
          })

          //TODO: index spark plan 执行条件下推优化
          // build spark plan 则不执行
          val indexSparkPlan = planLater(join.left)
          val buildSparkPlan = planLater(join.right)

          // condition 执行 时区替换、常量替换、UnresolvedFunction解析 等优化
          val resolvedCondition = join.condition.map(exp => {
            ExpressionTransformHelper.resolveExpression(spark, new SQLConf)(exp)
          })

          // sets 执行 时区替换、常量替换、UnresolvedFunction解析 等优化
          val resolvedSets = newSets.map(tp => {
            val e2 = ExpressionTransformHelper.resolveExpression(spark, new SQLConf)(tp._2)
            val e21 = e2 transformUp {
              case alias @ Alias(child, name) =>
                AttributeReference(name, alias.dataType, alias.nullable)(alias.exprId, alias.qualifier)
            }
            (tp._1 -> e2)
          })

          // build 与 index 的区别是，index (to be updated relation) 不会有二级子查询
          val buildWithPushdown = pushdownPredictOnRight(join, join.condition.get)
          val indexWithPushdown = pushdwonPredictOnLeft(join, resolvedCondition.get)

          val resolvedJoin = resolveJoinLogicalPlan(join)
          // 提取 update where A=B 中，A 和 B 中的 keys ( 在 index 和 build relation 中的字段 )
          val indexPlanKeys = resolvedJoin._2
          val buildPlanKeys = resolvedJoin._3
          val updateOpType = makeUpdateOperationType(targetTableDesc, indexPlanKeys)

          val allOutput = indexSparkPlan.output ++ buildSparkPlan.output

          // 1） 如果 update relation 支持增量更新 （ 能根据某个主键查询 items ，只更新查询到的 items ）
          // 可以使用 IndexHashJoinUpdate
          val indexHashJoin = IndexedHashJoinExec(buildPlanKeys, indexPlanKeys,
            allOutput, resolvedCondition, buildSparkPlan, indexSparkPlan, indexWithPushdown)

          // 2)  否则，只能使用全量扫描、更新部分记录，全量写入的方式进行 update
          val fullMergeHashJoin = FullMergeBroadcastHashJoinExec(buildPlanKeys, indexPlanKeys,
            allOutput, resolvedCondition, buildSparkPlan, indexSparkPlan, resolvedSets)

          updateOpType match {
            case UpdateOperationType.IndexedUpdate => {
              IndexedHashJoinUpdateExec(u.input, allOutput, u.target, resolvedSets, indexHashJoin, buildPlanKeys) :: Nil
              //BroadcastHashJoinUpdateExec(u.input, u.output, u.target, resolvedSets, resolvedCondition, planLater(join), buildPlanKeys) :: Nil
              //OneStageUpdateExec(u.input, allOutput, u.target, resolvedSets,buildPlanKeys,indexPlanKeys, resolvedCondition,buildSparkPlan, indexSparkPlan, indexWithPushdown) :: Nil
            }
            case _ => {
              //FMHJoinUpdateExec(u.input, allOutput, u.target, resolvedSets, resolvedCondition, fullMergeHashJoin, buildPlanKeys, indexSparkPlan.output) :: Nil
              IndexedHashJoinUpdateExec(u.input, allOutput, u.target, resolvedSets, indexHashJoin, buildPlanKeys) :: Nil
              //BHJoinUpdateExec(u.input, u.output, u.target, resolvedSets, resolvedCondition,
              //  planLater(join), buildPlanKeys) :: Nil
              //OneStageUpdateExec(u.input, allOutput, u.target, resolvedSets,buildPlanKeys,indexPlanKeys, resolvedCondition,buildSparkPlan, indexSparkPlan, indexWithPushdown) :: Nil
            }
          }
        }
      }

      case _ => Seq.empty
    }
  }
}




/*

def findOriginalAttributeByExprId(exprIds: Seq[Expression], dest: ExprId) : Expression = {
            val out = exprIds.filter(x => {
              val matched = x match {
                case alias @ Alias(child, name) => dest.equals(alias.exprId)
                case attr : Attribute => dest.equals(attr.exprId)
                case _ => false
              }
              matched
            })

            if (out.isEmpty) null else {
              out.apply(0) match {
                case alias @ Alias(child, name) => child
                case _ => out.apply(0)
              }
            }
          }

          def reverseAttributeByAlias(orgExp: Expression, exprIds: Seq[Expression]) : Expression = {
            val down = orgExp transform {
              case attr : AttributeReference => {
                val matchedAttr = findOriginalAttributeByExprId(exprIds, attr.exprId)
                if (matchedAttr == null) {
                  attr
                } else {
                  matchedAttr
                }
              }
            }

            down
          }

          def pushdownPredictOnRight(join: Join): LogicalPlan = {
            join.right match {
              case alias @ SubqueryAlias(id, pj @ Project(projectList, child)) => {
                val downCond = reverseAttributeByAlias(join.condition.get, projectList)
                SubqueryAlias(id, Project(projectList, Filter((downCond), child)))
              }
              case pj @ Project(projectList, child) => {
                val downCond = reverseAttributeByAlias(join.condition.get,projectList)
                Project(projectList, Filter((downCond), child))
              }
              case _ => throw new IllegalArgumentException(s"condition : ${u.originCondition} is error!")
            }
          }

          def pushdwonPredictOnLeft(join: Join, condition: Expression): LogicalPlan = {
            join.left match {
              case alias @ SubqueryAlias(id, pj @ Project(projectList, child)) => {
                SubqueryAlias(id, Project(projectList, Filter(condition, child)))
              }
              case alias @ SubqueryAlias(id, child) => {
                SubqueryAlias(id, Filter(condition, child))
              }
              case pj @ Project(projectList, child) => {
                Project(projectList, Filter(condition, child))
              }
              case filter @ Filter(cond, child) => {
                Filter(And(condition, cond), child)
              }
              case _ => throw new IllegalArgumentException(s"condition : ${u.originCondition} is error!")
            }
          }

 */