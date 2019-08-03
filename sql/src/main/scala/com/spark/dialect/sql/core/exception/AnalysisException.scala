package com.spark.dialect.sql.core.exception

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by yilong on 2019/1/17.
  */
class AnalysisException (val message: String,
                         val line: Option[Int] = None,
                         val startPosition: Option[Int] = None,
                         // Some plans fail to serialize due to bugs in scala collections.
                         @transient val plan: Option[LogicalPlan] = None,
                         val cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull) with Serializable {

  def withPosition(line: Option[Int], startPosition: Option[Int]): AnalysisException = {
    val newException = new AnalysisException(message, line, startPosition)
    newException.setStackTrace(getStackTrace)
    newException
  }

  override def getMessage: String = {
    val planAnnotation = Option(plan).flatten.map(p => s";\n$p").getOrElse("")
    getSimpleMessage + planAnnotation
  }

  // Outputs an exception without the logical plan.
  // For testing only
  def getSimpleMessage: String = {
    val lineAnnotation = line.map(l => s" line $l").getOrElse("")
    val positionAnnotation = startPosition.map(p => s" pos $p").getOrElse("")
    s"$message;$lineAnnotation$positionAnnotation"
  }
}
