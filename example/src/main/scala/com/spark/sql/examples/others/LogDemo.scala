package com.spark.sql.examples.others

import com.example.srpc.nettyrpc.util.ThreadUtils



/**
  * Created by yilong on 2019/6/6.
  */
class MyLog extends org.apache.spark.internal.Logging{
  def doLog(): Unit = {
    logInfo("hi")
  }
}

object LogDemo {
  def debugLogs(): Unit = {
    val ml = new MyLog
    val thds = ThreadUtils.newDaemonFixedThreadPool(100, "")
    (0 to 90).foreach(x => {
      thds.execute(new Runnable {
        override def run(): Unit = {
          (0 to 1000000).foreach(x => {
            ml.doLog()
          })
        }
      })
    })

    thds.awaitTermination(10000, java.util.concurrent.TimeUnit.SECONDS)
    thds.shutdown()
  }

  def main(args: Array[String]): Unit = {
    val str = "a.b"
    System.out.println(str.contains("\\."))
    val out = str.split("\\.")
    out.foreach(x => System.out.println(x))
  }
}
