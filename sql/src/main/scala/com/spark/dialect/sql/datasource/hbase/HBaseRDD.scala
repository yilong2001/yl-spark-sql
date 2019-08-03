package com.spark.dialect.sql.datasource.hbase

import java.io.IOException
import java.nio.charset.Charset
import java.util
import java.util.Map
import java.util.function.{BiConsumer, Consumer}

import com.spark.dialect.sql.util.HBaseUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName, filter}
import org.apache.hadoop.hbase.filter.{BinaryComparator, BinaryPrefixComparator, CompareFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}

import scala.reflect.internal.util.ScalaClassLoader
import scala.util.control.NonFatal
import scala.util.control._
import scala.collection.JavaConverters._

/**
  * Created by yilong on 2018/6/7.
  *
  */
//TODO: foreach 使用 immutuable 优化
case class HBasePartition(startRowKey: String, endRowKey: String, idx: Int) extends Partition {
  override def index: Int = idx
}

class HBaseRDD(sc: SparkContext,
               option: HBaseOption,
               filters: Array[Filter])
  extends RDD[InternalRow](sc, Nil) {

  hbaserdd =>

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var conn : Connection = null
    var tableInterface : Table = null
    var resultScanner : ResultScanner = null

    //val tuples = buildRowkeyAndValueFilter(options.rowkeyTransfer, filters)
    //val avroFilter = new SingleAvroFilter(options.family.getBytes(Charset.forName("utf-8")),
    //  tuples._1, tuples._2, options.tableAvsc)

    //TODO: update relation statis
    def close() {
      logInfo(" ***********************  HBaseRDD : compute : close  ************************ ")

      if (closed) return
      try {
        resultScanner.close()
      } catch {
        case e : Exception => logWarning("hbase result scanner close", e)
      }

      try {
        if (tableInterface != null) {
          tableInterface.close()
        }
      } catch {
        case e : Exception => logWarning("hbase table close ", e)
      }

      try {
        if (conn != null) {
          conn.close()
        }
      } catch {
        case e : Exception => logWarning("hbase connection close ", e)
      }

      logInfo("closed connection")
      closed = true
    }

    context.addTaskCompletionListener{ context => close() }

    val scan = new Scan

    //TODO : scan.setMaxVersions.setBatch(2).setCaching(hbaseOptions.)
    //TODO : 列裁剪
    //scan.setFilter(avroFilter)
    scan.setCaching(1000)

    try {
      conn = ConnectionFactory.createConnection(HBaseUtils.getConfiguration(option))
      tableInterface = conn.getTable(TableName.valueOf(option.hTableName))
      resultScanner = tableInterface.getScanner(scan)

      //if (schema == null) {
      //  logError("avro schema should not be null")
      //  throw new IOException("avro schema should not be null")
      //}

      //val rowsIterator = HBaseUtils.resultSetToSparkInternalRows(option, resultScanner, schema)
      //CompletionIterator[InternalRow, Iterator[InternalRow]](
      //  new InterruptibleIterator(context, rowsIterator), close())
      //TODO:
      null
    } catch {
      case e : IOException => logError(e.getMessage, e); throw e
    }
  }

  override protected def getPartitions: Array[Partition] = {
    HBaseUtils.getPartitions(option)
  }
}

object HBaseRDD {
    def scanTable(sc : SparkContext,
                  requiredColumns: Array[String],
                  filters: Array[Filter],
                  parts: Array[Partition],
                  hbaseOption: HBaseOption) : RDD[InternalRow] = {
  //    val (hbaseFilterList, valueCondition) = buildRowkeyAndValueFilter(hbaseOptions.rowkeyTransfer, filters)
  //
  //    val avroSchema = new Schema.Parser().parse(hbaseOptions.tableAvsc)
  //    val schema = AvroSchema.transferAvroSchema(avroSchema)
  //
  //    val avroFilter = new SingleAvroFilter(hbaseOptions.family.getBytes(Charset.forName("utf-8")),
  //      hbaseFilterList, valueCondition, hbaseOptions.tableAvsc)


      new HBaseRDD(sc, hbaseOption, filters)
    }
}
