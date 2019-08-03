package com.spark.dialect.ml

import java.io._
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.spark.dialect.generated.proto.{ModelAlgorithm, ModelAlgorithmServiceGrpc}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

/**
  * Created by yilong on 2019/8/1.
  */
object BasePrediction {
  final val _logger = Logger.getLogger("BasePrediction")

  val channelThreadLocal = new ThreadLocal[ManagedChannel]()

  def buildManagedChannel(algoServer: String) : ManagedChannel = {
    val host = algoServer.split(":").apply(0)
    val port = algoServer.split(":").apply(1).toInt
    val channel = ManagedChannelBuilder
      .forAddress(host, port)
      .usePlaintext()
      .build()

    channel
  }

  def buildModelAlgorithmServiceGrpcStub(channel: ManagedChannel) : ModelAlgorithmServiceGrpc.ModelAlgorithmServiceBlockingStub = {
    val stub = ModelAlgorithmServiceGrpc
      .newBlockingStub(channel)
      .withDeadlineAfter(20000, TimeUnit.MILLISECONDS)

    stub
  }

  def dataTypeCode(dt: DataType): Byte = {
    dt match {
      case IntegerType => 0
      case DoubleType => 1
      case FloatType => 2
      case ByteType => 3
      case BooleanType => 4
      case ShortType => 5
      case LongType => 6
      case _ => throw new IllegalArgumentException(dt.typeName + " is not supported")
    }
  }

  def serializeGenricRow(row: GenericRowWithSchema, outstream: DataOutputStream) : Unit = {
    val fds = row.schema.fields
    outstream.write(fds.length)
    fds.foreach(fd => {
      outstream.writeByte(dataTypeCode(fd.dataType))
    })
    row.toSeq.zip(fds).foreach(rv => {
      rv._2.dataType match {
        case IntegerType => outstream.writeInt(rv._1.asInstanceOf[Int])
        case DoubleType => outstream.writeDouble(rv._1.asInstanceOf[Double])
        case FloatType => outstream.writeFloat(rv._1.asInstanceOf[Float])
        case ByteType => outstream.writeByte(rv._1.asInstanceOf[Byte])
        case BooleanType =>  outstream.writeBoolean(rv._1.asInstanceOf[Boolean])
        case ShortType =>  outstream.writeShort(rv._1.asInstanceOf[Short])
        case LongType =>  outstream.writeLong(rv._1.asInstanceOf[Long])
        case _ => throw new IllegalArgumentException(rv._2.dataType.typeName + " is not supported")
      }
    })

    outstream.flush()
  }

  def deserializePrediction(instream: DataInputStream): Any = {
    val typeCode = instream.readByte()
    typeCode match {
      case 0 => instream.readInt()
      case 1 => instream.readDouble()
      case 2 => instream.readFloat()
      case 3 => instream.readByte()
      case 4 => instream.readBoolean()
      case 5 => instream.readShort()
      case 6 => instream.readLong()
      case _ => throw new IllegalArgumentException(typeCode + " is not supported")
    }
  }
}

class LogisticRegressionPrediction(modelFile: String) extends Serializable {
  import BasePrediction._

  def predict(arg: Any) : Int = {
    //if (channelThreadLocal.get() == null) {
    //  channelThreadLocal.set(BasePrediction.buildManagedChannel("localhost:29000"))
    //}

    val channel = BasePrediction.buildManagedChannel("localhost:29000") //channelThreadLocal.get()
    val stub = BasePrediction.buildModelAlgorithmServiceGrpcStub(channel)

    val row = arg.asInstanceOf[GenericRowWithSchema]

    val stream = new ByteArrayOutputStream;
    val dataOut = new DataOutputStream(stream)

    serializeGenricRow(row, dataOut)

    val req = ModelAlgorithm.ModelAlgorithmPredictRequest
      .newBuilder()
      .setModelFile(modelFile)
      .setFeatures(ByteString.copyFrom(stream.toByteArray))
      .build()

    var result : Int = 0
    try {
      val rsp = stub.predict(req)

      val instream = new ByteArrayInputStream(rsp.getResult.toByteArray);
      val dataIn = new DataInputStream(instream)

      result = deserializePrediction(dataIn).asInstanceOf[Int]
    } finally {
      try {
        channel.shutdownNow()
      } catch {
        case e: Exception => _logger.error(e.getMessage, e)
      }
    }

    result
  }
}

