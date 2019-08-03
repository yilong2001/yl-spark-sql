package com.spark.dialect.sql.core.execution.command

import com.spark.dialect.ml.LogisticRegressionPrediction
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.CreatableRelationProvider

/**
  * Created by yilong on 2019/8/1.
  */
object mlearn {

}

case class RegisterModelFunctionCommand(model: String, path: String, func: String) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    model match {
      case "LogisticRegression" => sparkSession.udf.register(func,
        new LogisticRegressionPrediction(path).predict _)
      case _ => throw new IllegalArgumentException(s"${model} is not support!")
    }

    Seq.empty[Row]
  }

  override def simpleString: String = {
    s"RegisterModelFunctionCommand ${model}, ${path}, ${func}"
  }
}