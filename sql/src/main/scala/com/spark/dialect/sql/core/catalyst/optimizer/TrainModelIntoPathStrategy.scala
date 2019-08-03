package com.spark.dialect.sql.core.catalyst.optimizer

import javax.management.relation.Relation

import com.spark.dialect.sql.core.catalyst.logical.TrainModelIntoPath
import com.spark.dialect.sql.core.execution.train.TrainModelIntoPathExec
import com.spark.dialect.sql.util.MetaConstants
import org.apache.log4j.Logger
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo, ExprId, Expression, Literal, Not, Or, RowOrdering}
import org.apache.spark.sql.execution.joins.BuildSide
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType, logical}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by yilong on 2019/7/28.
  */

object TrainModelIntoPathStrategy {
  final val logger = Logger.getLogger(classOf[TrainModelIntoPathStrategy])
  def info(obj:AnyRef) : Unit = {
    logger.info(obj)
  }
}

class TrainModelIntoPathStrategy(spark : SparkSession) extends sql.Strategy {
  import TrainModelIntoPathStrategy._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case u : TrainModelIntoPath => {
        withOrigin(u.origin) {
          info(s"TrainModelIntoPathStrategy : TrainModelIntoPath : ${u}")

          TrainModelIntoPathExec(u.output, planLater(u.child), u.target, u.algorithmType) :: Nil
        }
      }

      case _ => Seq.empty
    }
  }
}

