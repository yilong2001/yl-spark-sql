package com.spark.sql.examples.others

import org.apache.spark.deploy.{SparkSubmit, SparkSubmitArguments}
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.util.{CommandLineUtils, Utils}

/**
  * Created by yilong on 2019/7/22.
  */
object PyDemo1 {
  def main(args: Array[String]): Unit = {
    val clArgs = Array(
      "--verbose",
    "/Users/yilong/research/ml/demo/pyspark_lr_demo.py",
    "/Users/yilong/research/ml/datasets/finicial/kaggle/part_log.csv")

    SparkSubmit.main(clArgs)
  }

}
