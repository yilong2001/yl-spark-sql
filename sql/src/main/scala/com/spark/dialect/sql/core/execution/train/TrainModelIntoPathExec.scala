package com.spark.dialect.sql.core.execution.train

import java.io.{ByteArrayOutputStream, DataOutputStream, OutputStream}
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.spark.dialect.generated.proto.{ModelAlgorithm, ModelAlgorithmServiceGrpc}
import com.spark.dialect.sql.core.catalyst.logical.TrainAlgorithmType.TrainAlgorithmType
import com.spark.dialect.sql.util.{CommonUtils, MetaConstants}
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, BoundReference, Expression, JoinedRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
  * Created by yilong on 2019/6/8.
  */

object ArrowDataWriter {
  final val _logger = Logger.getLogger("ArrowDataWriter")

  def writeToStream(inputIterator: Iterator[InternalRow],
                    schema: StructType,
                    timeZoneId: String,
                    dataOut: OutputStream,
                    max: Int):Unit = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator(
      s"data writer to Model Algorithm Server", 0, Long.MaxValue)
    val root = VectorSchemaRoot.create(arrowSchema, allocator)

    CommonUtils.tryWithSafeFinally {
      val arrowWriter = ArrowWriter.create(root)
      val writer = new ArrowStreamWriter(root, null, dataOut)
      writer.start()

      var count = 0
      while (inputIterator.hasNext && (count < max)) {
        count = count + 1
        arrowWriter.write(inputIterator.next())
      }

      arrowWriter.finish()
      writer.writeBatch()
      arrowWriter.reset()

      // end writes footer to the output stream and doesn't clean any resources.
      // It could throw exception if the output stream is closed, so it should be
      // in the try block.
      writer.end()
    } {
      root.close()
      allocator.close()
    }
  }

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
}

case class TrainModelIntoPathExec(output: Seq[Attribute],
                                  child: SparkPlan,
                                  target: String,
                                  algorithmType: TrainAlgorithmType)
  extends org.apache.spark.sql.execution.UnaryExecNode {
  final val logger = Logger.getLogger("TrainModelIntoPathExec")
  final val MAX_COUNT_PER_GRPC = 3000

  override def outputPartitioning: Partitioning = child.outputPartitioning

  //TODO:
  override def requiredChildDistribution: Seq[Distribution] = {
    UnspecifiedDistribution :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logger.info(s"************ doExecute:: output : ${output}; child: ${child} ")
    val sparkSession = sqlContext.sparkSession
    val timeZoneId = sqlContext.sparkSession.sessionState.conf.sessionLocalTimeZone
    val schema = child.schema
    val taskid = UUID.randomUUID().toString

    //TODO: only support one server
    val algoServer = sparkSession.conf.get(MetaConstants.MEAT_DATA_TRAIN_MODEL_ALGORITHM_SERVERS).split(";").apply(0)

    child.execute().mapPartitions {
      it => {
        val count = 0
        //val _log = Logger.getLogger("TrainModelIntoPathExec")
        val channel = ArrowDataWriter.buildManagedChannel(algoServer)
        while (it.hasNext) {
          val outputStream = new ByteArrayOutputStream()
          ArrowDataWriter.writeToStream(it, schema, timeZoneId, outputStream, MAX_COUNT_PER_GRPC)

          val bytes = outputStream.toByteArray

          val datareq = ModelAlgorithm.ModelAlgorithmDataRequest.newBuilder()
            .setTaskId(taskid)
            .setBatchData(ByteString.copyFrom(bytes))
            .setBatchSize(bytes.size)
            .build()

          val stub = ArrowDataWriter.buildModelAlgorithmServiceGrpcStub(channel)
          try {
            var rsp = stub.sendBatch(datareq)
            if (rsp.getStatus.equals(ModelAlgorithm.ModelAlgorithmResponse.STATUS.BUSY)) {
              Thread.sleep(3000)
            }
            rsp = stub.sendBatch(datareq)
            ArrowDataWriter._logger.info(s"send batch rsp : ${rsp}")
          } catch {
            case e: Exception => {
              ArrowDataWriter._logger.error(e.getMessage, e)
            }
            case th: Throwable => {
              ArrowDataWriter._logger.error(th.getMessage, th)
            }
          } finally {

          }
        }

        try {
          channel.shutdown()
        } catch {
          case e:Exception => ArrowDataWriter._logger.error(e.getMessage, e)
        }

        it
      }
    }.take(1)

    val stub = ArrowDataWriter.buildModelAlgorithmServiceGrpcStub(ArrowDataWriter.buildManagedChannel(algoServer))
    val computereq = ModelAlgorithm.ModelAlgorithmComputeRequest.newBuilder()
      .setTaskId(taskid)
      .setTargetFile(target)
      .build()
    val cleanrep = ModelAlgorithm.ModelAlgorithmCleanRequest.newBuilder()
      .setTaskId(taskid)
      .build()

    try {
      val rsp = stub.computeLR(computereq)
    } catch {
      case e : Exception => {
        logError(e.getMessage, e)
      }
      case th: Throwable => {
        logError(th.getMessage, th)
      }
    }

    try {
      val rsp = stub.cleanBatch(cleanrep)
    } catch {
      case e : Exception => {
        logError(e.getMessage, e)
      }
      case th: Throwable => {
        logError(th.getMessage, th)
      }
    }

    //Seq.empty[InternalRow]
    child.execute().mapPartitions {
      it => Iterator.empty
    }
  }
}
