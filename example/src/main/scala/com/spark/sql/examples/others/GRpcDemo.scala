package com.spark.sql.examples.others

import java.util.concurrent.TimeUnit

import com.google.protobuf.{BlockingRpcChannel, ByteString}
import com.spark.dialect.generated.proto.PbExample._
import com.spark.dialect.generated.proto._
import com.spark.sql.examples.ArrowWrite2Bytes
import io.grpc.ManagedChannelBuilder



/**
  * Created by yilong on 2019/7/23.
  */
object GRpcDemo {

  def main(args: Array[String]): Unit = {
    val bytes = new Array[Byte](10)

    val aw2b = new ArrowWrite2Bytes()

    val bds = aw2b.writeData()

    val datarequest = PbExample.PbExampleDataRequest.newBuilder()
      .setSchema(aw2b.getSchema)
      .setTaskId("1")
      .setBatchSize(100)
      .setBatchData(ByteString.copyFrom(bds))
      .build()

    val msgrequest = PbExample.PbExampleComputeRequest.newBuilder()
      .setTaskId("1")
      .setTargetFile("/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/exdata/lrmodel.dat")
      .build()

    val channel = ManagedChannelBuilder
      .forAddress("localhost", 29000)
      .usePlaintext()
      .build();

    val stub = PbExampleServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(5000, TimeUnit.MILLISECONDS)

    val num = 2
    val threads1 = (0 to num).map(x => new Thread {
      override def run(): Unit = {
        val rsp = stub.sendBatch(datarequest)
        println(rsp)
      }
    })

    val threads2 = (0 to 0).map(x => new Thread {
      override def run(): Unit = {
        val rsp = stub.compute(msgrequest)
        println(rsp)
      }
    })

    threads1.foreach(t => t.start())
    threads1.foreach(t => t.join())

    threads2.foreach(t => t.start())
    threads2.foreach(t => t.join())
  }
}
