package com.spark.dialect.sql.util

import java.io.{PrintWriter, StringWriter}

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateMutableProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{Alias, EmptyRow, Expression, Nondeterministic, Projection, UnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
  * Created by yilong on 2019/5/19.
  */
object ExpressionEvalHelper {
  final val _log = Logger.getLogger("ExpressionEvalHelper")
  def prepareEvaluation(expression: Expression): Expression = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance
    val resolver = ResolveTimeZone(new SQLConf)
    //resolver.resolveTimeZones(serializer.deserialize(serializer.serialize(expression)))
    resolver.resolveTimeZones(expression)
  }

  def evaluateWithGeneratedMutableProjection(expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    val plan = generateProject(
      GenerateMutableProjection.generate(Alias(expression, s"Optimized($expression)")() :: Nil),
      expression)
    plan.initialize(0)

    plan(inputRow).get(0, expression.dataType)
  }

  def evaluateWithUnsafeProjection(expression: Expression, inputRow: InternalRow = EmptyRow): InternalRow = {
    // SPARK-16489 Explicitly doing code generation twice so code gen will fail if
    // some expression is reusing variable names across different instances.
    // This behavior is tested in ExpressionEvalHelperSuite.
    val plan = generateProject(
      UnsafeProjection.create(
        Alias(expression, s"Optimized($expression)1")() ::
          Alias(expression, s"Optimized($expression)2")() :: Nil),
      expression)

    plan.initialize(0)
    plan(inputRow)
  }

  def generateProject(generator: => Projection, expression: Expression): Projection = {
    try {
      generator
    } catch {
      case e: Throwable => {
        _log.error(e.getMessage, e)
        throw e
      }
    }
  }
}
