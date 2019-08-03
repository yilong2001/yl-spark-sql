package com.spark.dialect.sql.util

import org.apache.spark.sql.SparkSession

import scala.language.experimental.macros
import scala.reflect.macros.whitebox.Context

/**
  * Created by yilong on 2019/4/27.
  */
object RegMacroImpl {
  def appl(c: Context)(spark: c.Tree, regs: c.Tree) = {
    import c.universe._
    q"""${regs}"""
  }
}

object RegMacro {
  def apply(spark:SparkSession, regs: String): Any = macro RegMacroImpl.appl
}
