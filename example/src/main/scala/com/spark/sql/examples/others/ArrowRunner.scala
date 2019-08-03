package com.spark.sql.examples.others

import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

import com.spark.dialect.sql.util.CommonUtils
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.{ArrowUtils, ArrowWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

/**
  * Created by yilong on 2019/7/30.
  */
class ArrowRunner {

}
class ArrowPythonRunner(evalType: Int,
                         argOffsets: Array[Array[Int]],
                         schema: StructType,
                         timeZoneId: String,
                         conf: Map[String, String])
  extends BasePythonRunner[Iterator[InternalRow], ColumnarBatch](evalType) {

  override def newWriterThread(worker: Socket,
                                          inputIterator: Iterator[Iterator[InternalRow]],
                                          partitionIndex: Int): WriterThread = {
    new WriterThread(worker, inputIterator, partitionIndex) {

      override def writeCommand(dataOut: DataOutputStream): Unit = {
        // Write config for the worker as a number of key -> value pairs of strings
        //dataOut.writeInt(conf.size)
        //for ((k, v) <- conf) {
        //  PythonRDD.writeUTF(k, dataOut)
        //  PythonRDD.writeUTF(v, dataOut)
        //}

        //PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets)
      }

      override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        val arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId)
        val allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"stdout writer for $pythonExec", 0, Long.MaxValue)
        val root = VectorSchemaRoot.create(arrowSchema, allocator)

        CommonUtils.tryWithSafeFinally {
          val arrowWriter = ArrowWriter.create(root)
          val writer = new ArrowStreamWriter(root, null, dataOut)
          writer.start()

          while (inputIterator.hasNext) {
            val nextBatch = inputIterator.next()

            while (nextBatch.hasNext) {
              arrowWriter.write(nextBatch.next())
            }

            arrowWriter.finish()
            writer.writeBatch()
            arrowWriter.reset()
          }
          // end writes footer to the output stream and doesn't clean any resources.
          // It could throw exception if the output stream is closed, so it should be
          // in the try block.
          writer.end()
        } {
          // If we close root and allocator in TaskCompletionListener, there could be a race
          // condition where the writer thread keeps writing to the VectorSchemaRoot while
          // it's being closed by the TaskCompletion listener.
          // Closing root and allocator here is cleaner because root and allocator is owned
          // by the writer thread and is only visible to the writer thread.
          //
          // If the writer thread is interrupted by TaskCompletionListener, it should either
          // (1) in the try block, in which case it will get an InterruptedException when
          // performing io, and goes into the finally block or (2) in the finally block,
          // in which case it will ignore the interruption and close the resources.
          root.close()
          allocator.close()
        }
      }
    }
  }

  def newReaderIterator(stream: DataInputStream,
                                            writerThread: WriterThread,
                                            startTime: Long,
                                            worker: Socket,
                                            releasedOrClosed: AtomicBoolean): Iterator[ColumnarBatch] = {
    new ReaderIterator(stream, writerThread, startTime, worker, releasedOrClosed) {

      private val allocator = ArrowUtils.rootAllocator.newChildAllocator(
        s"stdin reader for $pythonExec", 0, Long.MaxValue)

      private var reader: ArrowStreamReader = _
      private var root: VectorSchemaRoot = _
      private var schema: StructType = _
      private var vectors: Array[ColumnVector] = _

      //context.addTaskCompletionListener[Unit] { _ =>
      //  if (reader != null) {
      //    reader.close(false)
      //  }
      //  allocator.close()
      //}

      private var batchLoaded = true

      override def read(): ColumnarBatch = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          if (reader != null && batchLoaded) {
            batchLoaded = reader.loadNextBatch()
            if (batchLoaded) {
              val batch = new ColumnarBatch(vectors)
              batch.setNumRows(root.getRowCount)
              batch
            } else {
              reader.close(false)
              allocator.close()
              // Reach end of stream. Call `read()` again to read control data.
              read()
            }
          } else {
            stream.readInt() match {
              case SpecialLengths.START_ARROW_STREAM =>
                reader = new ArrowStreamReader(stream, allocator)
                root = reader.getVectorSchemaRoot()
                schema = ArrowUtils.fromArrowSchema(root.getSchema())
                vectors = root.getFieldVectors().asScala.map { vector =>
                  new ArrowColumnVector(vector)
                }.toArray[ColumnVector]
                read()
              case SpecialLengths.TIMING_DATA =>
                handleTimingData()
                read()
              case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
                throw handlePythonException()
              case SpecialLengths.END_OF_DATA_SECTION =>
                handleEndOfDataSection()
                null
            }
          }
        } catch handleException
      }
    }
  }
}
