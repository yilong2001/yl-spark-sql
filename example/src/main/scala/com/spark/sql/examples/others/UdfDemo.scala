package com.spark.sql.examples.others

import java.io._
import java.util.concurrent.TimeUnit

import com.google.protobuf.ByteString
import com.spark.dialect.generated.proto.{ModelAlgorithm, ModelAlgorithmServiceGrpc}
import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1
import com.spark.dialect.sql.core.catalyst.optimizer.{TrainModelIntoPathStrategy, UpdateSetOperationStrategy}
import com.spark.dialect.sql.core.catalyst.rules.UpdateSetOperationRules
import com.spark.dialect.sql.util.{MetaConstants, SerdeUtils}
import com.spark.sql.examples.JUdfEx
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, Literal}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.{Column, SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by yilong on 2019/7/31.
  */
object BasePrediction {
  val threadChannel = new ThreadLocal[ManagedChannel]()
  //val threadStub = new ThreadLocal[ModelAlgorithmServiceGrpc.ModelAlgorithmServiceBlockingStub]()

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

class BasePrediction(modelFile: String) extends Serializable {
  import BasePrediction._
  var count = 0
  def predict(arg: Any) : Int = {
    if (threadChannel.get() == null) {
      threadChannel.set(BasePrediction.buildManagedChannel("localhost:29000"))
    }

    val channel = threadChannel.get()//BasePrediction.buildManagedChannel("localhost:29000")//threadChannel.get()

    val stub = ModelAlgorithmServiceGrpc.newBlockingStub(channel)

    //println(arg.getClass)
    val row = arg.asInstanceOf[GenericRowWithSchema]

    val stream = new ByteArrayOutputStream;
    val dataOut = new DataOutputStream(stream)

    serializeGenricRow(row, dataOut)

    val req = ModelAlgorithm.ModelAlgorithmPredictRequest
      .newBuilder()
      .setModelFile(modelFile)
      .setFeatures(ByteString.copyFrom(stream.toByteArray))
      .build()

    //println(row)
    //println("------")
    //stream.toByteArray.foreach(x => {
    //  val y = if (x<0) 256+x else x
    //  print(y)
    //  print(",")
    //})
    //println("------")

    val rsp = stub.predict(req)

    //println(rsp)

    val instream = new ByteArrayInputStream(rsp.getResult.toByteArray);
    val dataIn = new DataInputStream(instream)

    val result = deserializePrediction(dataIn).asInstanceOf[Int]

    //println("result = " + result)
    count = count+1
    if (count > 10000) { //(true) {//
      count = 0
      threadChannel.remove()
      try {
        channel.shutdownNow()
      } catch {
        case e:Exception => e.printStackTrace()
      }
    }

    result
  }
}

object UdfDemo {
  def main(args: Array[String]): Unit = {
    val arr = Array[Any](12.908, 1, 0.091)
    val sarr = SerdeUtils.write(arr)
    //(sarr.foreach(println))
    val desarr = SerdeUtils.read[Array[Any]](sarr)
    (desarr.foreach(println))


    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    type RuleBuilder = SparkSession => Rule[LogicalPlan]
    type StrategyBuilder = SparkSession => Strategy

    val parserBuilder: ParserBuilder = (sparkSession, parser) => new ExtendSparkSqlParserV1(sparkSession, parser)
    val updateRuleBuilder: RuleBuilder = (sparkSession) => new UpdateSetOperationRules(sparkSession)
    val updateStrategyBuilder: StrategyBuilder = (sparkSession) => new UpdateSetOperationStrategy(sparkSession)
    val trainStrategyBuilder: StrategyBuilder = (sparkSession) => new TrainModelIntoPathStrategy(sparkSession)

    val extBuilder: ExtensionsBuilder = { e => {
      e.injectParser(parserBuilder)
      e.injectResolutionRule(updateRuleBuilder)
      e.injectPlannerStrategy(updateStrategyBuilder)
      e.injectPlannerStrategy(trainStrategyBuilder)
    }
    }

    val warehouseLocation = "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/dw"

    val conf = new SparkConf()
    conf.setMaster("local[5]")
    conf.setAppName("SqlExample")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/test")
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("spark.sql.warehouse.dir", warehouseLocation)
    //conf.set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    //conf.set("hive.metastore.uris", "thrift://localhost:9083")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.broadcastTimeout", "1000")
    //conf.set("spark.default.parallelism", "6")
    conf.set("spark.sql.shuffle.partitions", "2")

    conf.set(MetaConstants.MEAT_DATA_TRAIN_MODEL_ALGORITHM_SERVERS, "localhost:29000")
    //conf.registerKryoClasses(Array(classOf[UpdateSetOperation],classOf[IndexedHashJoinExec]))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .withExtensions(extBuilder)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("predict", new BasePrediction("/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/models/lrmodel.m").predict _)

    val sqlContext = spark.sqlContext

    val p002 =
      """
        | CREATE external TABLE IF NOT EXISTS tmp_p_part_log (
        | step int,
        | tcd int,
        | amount double,
        | oldbalanceOrg double,
        | newbalanceOrig double,
        | oldbalanceDest double,
        | newbalanceDest double,
        | errorBalanceOrig double,
        | errorBalanceDest double,
        | label int
        | )
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        | LOCATION '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/exdata'
      """.stripMargin
    sqlContext.sql(p002)

    val sql10 = "select count(1) as total,sum(case when (label = predict(struct(step,tcd,amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,errorBalanceOrig,errorBalanceDest))) then 1 else 0 end) as correct from tmp_p_part_log"
    val sql11 = "select label, predict(struct(step,tcd,amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,errorBalanceOrig,errorBalanceDest)) as predict from tmp_p_part_log"
    val sql12 = "select (label - predict(struct(step,tcd,amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,errorBalanceOrig,errorBalanceDest))) as eqs from tmp_p_part_log"
    //val sql2 = "select predict2(array(step,amount)) as features from p_part_log limit 1"
    //val sql3 = "select predict3(*) from p_part_log limit 1"
    val starttime = System.currentTimeMillis()
    sqlContext.sql(sql12)
    val endtime = System.currentTimeMillis()
    println(s"${endtime}, ${starttime}, ${endtime-starttime}")
  }
}
