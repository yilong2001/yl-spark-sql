package com.spark.sql.examples.others

import java.io._
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.util
import java.util.Arrays
import java.util.concurrent.atomic.AtomicBoolean

import com.spark.dialect.sql.util.CommonUtils
import org.apache.hadoop.hbase.wal.WALSplitter.WriterThread
import org.apache.log4j.Logger
import org.apache.spark.InterruptibleIterator

import scala.util.control.ControlThrowable


/**
  * Created by yilong on 2019/7/30.
  */
object PythonEvalType {
  val NON_UDF = 0

  val SQL_BATCHED_UDF = 100

  val SQL_SCALAR_PANDAS_UDF = 200
  val SQL_GROUPED_MAP_PANDAS_UDF = 201
  val SQL_GROUPED_AGG_PANDAS_UDF = 202
  val SQL_WINDOW_AGG_PANDAS_UDF = 203

  def toString(pythonEvalType: Int): String = pythonEvalType match {
    case NON_UDF => "NON_UDF"
    case SQL_BATCHED_UDF => "SQL_BATCHED_UDF"
    case SQL_SCALAR_PANDAS_UDF => "SQL_SCALAR_PANDAS_UDF"
    case SQL_GROUPED_MAP_PANDAS_UDF => "SQL_GROUPED_MAP_PANDAS_UDF"
    case SQL_GROUPED_AGG_PANDAS_UDF => "SQL_GROUPED_AGG_PANDAS_UDF"
    case SQL_WINDOW_AGG_PANDAS_UDF => "SQL_WINDOW_AGG_PANDAS_UDF"
  }
}

object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
  val START_ARROW_STREAM = -6
}

class RedirectThread(in: InputStream,
                     out: OutputStream,
                     name: String,
                     propagateEof: Boolean = false)
  extends Thread(name) {
  //setDaemon(true)
  override def run() {
    scala.util.control.Exception.ignoring(classOf[IOException]) {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      CommonUtils.tryWithSafeFinally {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}

class MyInterruptibleIterator[+T](val delegate: Iterator[T])
  extends Iterator[T] {

  def hasNext: Boolean = {
    // TODO(aarondav/rxin): Check Thread.interrupted instead of context.interrupted if interrupt
    // is allowed. The assumption is that Thread.interrupted does not have a memory fence in read
    // (just a volatile field in C), while context.interrupted is a volatile in the JVM, which
    // introduces an expensive read fence.
    //context.killTaskIfInterrupted()
    delegate.hasNext
  }

  def next(): T = delegate.next()
}

abstract class BasePythonRunner[IN, OUT](evalType: Int) {
  val logger = Logger.getLogger("BasePythonRunner")

  private val bufferSize = 65536
  private val reuseWorker = true
  var serverSocket: Option[ServerSocket] = None

  val envVars = new util.HashMap[String,String]()
  val pythonExec = "python3"
  val pythonVer = "3.6"
  val memoryMb = Option(512)

  def logUncaughtExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case ct: ControlThrowable =>
        throw ct
      case t: Throwable =>
        logger.error(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        throw t
    }
  }

  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + pythonExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + pythonExec).start()
    } catch {
      case e: Exception =>
        logger.error("Exception in redirecting streams", e)
    }
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  def createSimpleWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array[Byte](127, 0, 0, 1)))

      val pythonExec = "python3"
      val destpy = "/Users/yilong/setup/spark-2.4.0-bin-hadoop2.7/python/lib/dialectudf1.py"
      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(pythonExec, destpy))
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars)
      workerEnv.put("PYTHONPATH", "/Library/Frameworks/Python.framework/Versions/3.6/bin/python3")
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      //workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Wait for it to connect to our socket, and validate the auth secret.
      serverSocket.setSoTimeout(10000)

      try {
        val socket = serverSocket.accept()
        //authHelper.authClient(socket)
        //simpleWorkers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new IOException("Python worker failed to connect back.", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  def newWriterThread(worker: Socket,
                      inputIterator: Iterator[IN],
                      partitionIndex: Int): WriterThread

  def newReaderIterator(stream: DataInputStream,
                        writerThread: WriterThread,
                        startTime: Long,
                        worker: Socket,
                        releasedOrClosed: AtomicBoolean): Iterator[OUT]

  def compute(inputIterator: Iterator[IN],
              partitionIndex: Int) : Iterator[OUT] = {
    val startTime = System.currentTimeMillis
    if (reuseWorker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    if (memoryMb.isDefined) {
      envVars.put("PYSPARK_EXECUTOR_MEMORY_MB", memoryMb.get.toString)
    }

    val worker: Socket = createSimpleWorker()

    val releasedOrClosed = new AtomicBoolean(false)

    val writerThread = newWriterThread(worker, inputIterator, partitionIndex)
    writerThread.start()

    //new MonitorThread(env, worker, context).start()

    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))

    val stdoutIterator = newReaderIterator(stream, writerThread, startTime, worker, releasedOrClosed)

    // wait complete
    //writerThread.join()
    //try {
    //  worker.close()
    //} catch {
    //  case e: Exception =>
    //    logger.error("Failed to close worker socket", e)
    //}

    new MyInterruptibleIterator(stdoutIterator)
  }

  abstract class WriterThread(worker: Socket,
                     inputIterator: Iterator[IN],
                     partitionIndex: Int)
    extends Thread(s"stdout writer for") {

    @volatile private var _exception: Exception = null

    //private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    //private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    //setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      //assert(context.isCompleted)
      this.interrupt()
    }

    /**
      * Writes a command section to the stream connected to the Python worker.
      */
    protected def writeCommand(dataOut: DataOutputStream): Unit

    /**
      * Writes input data to the stream connected to the Python worker.
      */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit

    override def run(): Unit = logUncaughtExceptions {
      try {
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        writeUTF(pythonVer, dataOut)
        // Init a ServerSocket to accept method calls from Python side.

        val boundPort: Int = serverSocket.map(_.getLocalPort).getOrElse(0)
        if (boundPort == -1) {
          val message = "ServerSocket failed to bind to Java side."
          logger.error(message)
          throw new IOException(message)
        }

        // Write out the TaskContextInfo
        //dataOut.writeBoolean(isBarrier)
        dataOut.writeInt(boundPort)
        //val secretBytes = secret.getBytes(UTF_8)
        //dataOut.writeInt(secretBytes.length)
        //dataOut.write(secretBytes, 0, secretBytes.length)
        //dataOut.writeInt(context.stageId())
        //dataOut.writeInt(context.partitionId())
        //dataOut.writeInt(context.attemptNumber())
        //dataOut.writeLong(context.taskAttemptId())
        //val localProps = context.getLocalProperties.asScala
        //dataOut.writeInt(localProps.size)
        //localProps.foreach { case (k, v) =>
        //  PythonRDD.writeUTF(k, dataOut)
        //  PythonRDD.writeUTF(v, dataOut)
        //}

        // sparkFilesDir
        //PythonRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Python includes (*.zip and *.egg files)
        //dataOut.writeInt(pythonIncludes.size)
        //for (include <- pythonIncludes) {
        //  PythonRDD.writeUTF(include, dataOut)
        //}
        // Broadcast variables
        //val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        //val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        //val toRemove = oldBids.diff(newBids)
        //val addedBids = newBids.diff(oldBids)
        //val cnt = toRemove.size + addedBids.size
        //val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
        //dataOut.writeBoolean(needsDecryptionServer)
        //dataOut.writeInt(cnt)

        //def sendBidsToRemove(): Unit = {
        //  for (bid <- toRemove) {
        //    // remove the broadcast from worker
        //    dataOut.writeLong(-bid - 1) // bid >= 0
        //    oldBids.remove(bid)
        //  }
        //}

        //if (needsDecryptionServer) {
          // if there is encryption, we setup a server which reads the encrypted files, and sends
          // the decrypted data to python
        //  val idsAndFiles = broadcastVars.flatMap { broadcast =>
        //    if (!oldBids.contains(broadcast.id)) {
        //      Some((broadcast.id, broadcast.value.path))
        //    } else {
        //      None
        //    }
        //  }
        //  val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
        //  dataOut.writeInt(server.port)
        //  logTrace(s"broadcast decryption server setup on ${server.port}")
        //  PythonRDD.writeUTF(server.secret, dataOut)
        //  sendBidsToRemove()
        //  idsAndFiles.foreach { case (id, _) =>
            // send new broadcast
        //    dataOut.writeLong(id)
        //   oldBids.add(id)
        //  }
        //  dataOut.flush()
        //  logTrace("waiting for python to read decrypted broadcast data from server")
        //  server.waitTillBroadcastDataSent()
        //  logTrace("done sending decrypted data to python")
        //} else {
        //  sendBidsToRemove()
        //  for (broadcast <- broadcastVars) {
        //    if (!oldBids.contains(broadcast.id)) {
              // send new broadcast
        //      dataOut.writeLong(broadcast.id)
        //      PythonRDD.writeUTF(broadcast.value.path, dataOut)
        //      oldBids.add(broadcast.id)
        //    }
        //  }
        //}
        //dataOut.flush()

        dataOut.writeInt(evalType)
        writeUTF("/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/models/lrmodel.m", dataOut)
        //writeCommand(dataOut)
        writeIteratorToStream(dataOut)

        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception =>
          logger.error("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            (worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            (worker.shutdownOutput())
          }
      }
    }
  }

  abstract class ReaderIterator(stream: DataInputStream,
                                 writerThread: WriterThread,
                                 startTime: Long,
                                 worker: Socket,
                                 releasedOrClosed: AtomicBoolean)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    private var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
      * Reads next object from the stream.
      * When the stream reaches end of data, needs to process the following sections,
      * and then returns null.
      */
    protected def read(): OUT

    protected def handleTimingData(): Unit = {
      // Timing data from worker
      val bootTime = stream.readLong()
      val initTime = stream.readLong()
      val finishTime = stream.readLong()
      val boot = bootTime - startTime
      val init = initTime - bootTime
      val finish = finishTime - initTime
      val total = finishTime - startTime
      logger.info("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
        init, finish))
      val memoryBytesSpilled = stream.readLong()
      val diskBytesSpilled = stream.readLong()
    }

    protected def handlePythonException(): IOException = {
      // Signals that an exception has been thrown in python
      val exLength = stream.readInt()
      val obj = new Array[Byte](exLength)
      stream.readFully(obj)
      new IOException(new String(obj, StandardCharsets.UTF_8),
        writerThread.exception.getOrElse(null))
    }

    protected def handleEndOfDataSection(): Unit = {
      // We've finished the data section of the output, but we can still
      // read some accumulator updates:
      val numAccumulatorUpdates = stream.readInt()
      (1 to numAccumulatorUpdates).foreach { _ =>
        val updateLen = stream.readInt()
        val update = new Array[Byte](updateLen)
        stream.readFully(update)
        //accumulator.add(update)
      }
      // Check whether the worker is ready to be re-used.
      if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
        if (reuseWorker && releasedOrClosed.compareAndSet(false, true)) {
          //env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
          try {
            worker.close()
          } catch {
            case e: Exception =>
              logger.error("Failed to close worker socket", e)
          }
        }
      }
      eos = true
    }

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception => e.printStackTrace(); throw new IOException(e)
    }
  }
}


class PythonRunner() extends BasePythonRunner[Array[Byte], Array[Byte]](PythonEvalType.NON_UDF) {
  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      //case stream: PortableDataStream =>
      //  write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new IOException("Unexpected element type " + other.getClass)
    }

    iter.foreach(write)
  }

  override def newWriterThread(worker: Socket,
                               inputIterator: Iterator[Array[Byte]],
                               partitionIndex: Int): WriterThread = {
    new WriterThread(worker, inputIterator, partitionIndex) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        //val command = funcs.head.funcs.head.command
        //dataOut.writeInt(command.length)
        //dataOut.write(command)
      }

      protected override def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        //PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
      }
    }
  }

  override def newReaderIterator(stream: DataInputStream,
                                            writerThread: WriterThread,
                                            startTime: Long,
                                            worker: Socket,
                                            releasedOrClosed: AtomicBoolean): Iterator[Array[Byte]] = {
    new ReaderIterator(stream, writerThread, startTime, worker, releasedOrClosed) {

      protected override def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }

        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.TIMING_DATA =>
              handleTimingData()
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              throw handlePythonException()
            case SpecialLengths.END_OF_DATA_SECTION =>
              handleEndOfDataSection()
              null
          }
        } catch handleException
      }
    }
  }
}


object Jydemo {




  def main(args: Array[String]): Unit = {

  }
}
