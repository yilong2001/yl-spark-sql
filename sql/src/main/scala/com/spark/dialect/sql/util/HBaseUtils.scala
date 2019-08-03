package com.spark.dialect.sql.util

import java.io.IOException
import java.nio.charset.Charset

import com.spark.dialect.sql.core.execution.NextIterator
import com.spark.dialect.sql.datasource.hbase.{HBaseOption, HBasePartition}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yilong on 2018/6/8.
  */
object HBaseUtils extends Logging {

  def getPartitions(option: HBaseOption): Array[Partition] = {
    var conn: Connection = null
    val partitionArr = ArrayBuffer[HBasePartition]()
    try {
      conn = ConnectionFactory.createConnection(getConfiguration(option))

      val rl = conn.getRegionLocator(TableName.valueOf(option.hTableName))
      var id = 0
      if (rl == null || rl.getEndKeys == null || rl.getEndKeys.length == 0) {
        logError(option.hTableName + ", get region locator, but NULL. ")
        throw new IOException(option.hTableName + ", get region locator, but NULL (or endkeys is empty). ")
      }

      for (key : Array[Byte] <- rl.getEndKeys) {
        logInfo("rowkey : *****************************************")
        if (key == null || key.length == 0) {
          //TODO: id 应该更新？？？
          partitionArr += HBasePartition(null, null, id)
        }
        else {
          val start = (id==0) match {case true=>null; case _=> new String(rl.getEndKeys.apply(id))}
          partitionArr += HBasePartition(start, new String(key), id)
          id = id + 1
        }
      }

      partitionArr.toArray
    } catch {
      case e: IOException => {
        logError(e.getMessage, e)
        partitionArr.clear()
        partitionArr += HBasePartition(null, null, 0)

        partitionArr.toArray
      }
    } finally {
      if (conn != null) {
        try
          conn.close()
        catch {
          case e: IOException => logError(e.getMessage, e)
        } finally conn = null
      }
    }
  }

  def resultSetToSparkInternalRows(option: HBaseOption,
                                   resultScanner: ResultScanner,
                                   schema: StructType): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      private[this] val rs = resultScanner

      override protected def close(): Unit = {
        try {
          rs.close()
        } catch {
          case e: Exception => logWarning("Exception closing resultset", e)
        }
      }

      override protected def getNext(): InternalRow = {
        val result = resultScanner.next()
        if (result != null) {
          result
          val cells = List() //result.getColumnCells(option.family.getBytes(Charset.forName("utf-8")),
          //  option.family.getBytes(Charset.forName("utf-8")))
          if (cells == null || cells.size == 0) {
            finished = true
            null.asInstanceOf[InternalRow]
          } else {
            //TODO: how to support multi cell in cells
            //val data = cells.get(0).getValueArray
            //val obj = ThriftSerde.deSerialize(new NodeInfo(null, null).getClass/*option.valueClass*/, data)

            val keyValue = result.listCells().get(0)
            if (keyValue == null) {
              logError("keyvalue in cells(0) should not be null")
              throw new IOException("keyvalue in cells(0) should not be null")
            }

            //TODO: create new Row[]
            null
          }
        } else {
          finished = true
          null.asInstanceOf[InternalRow]
        }
      }
    }
  }

  def getSplits(option: HBaseOption) : List[Array[Byte]] = {
    logInfo(" ***********************  getSplits  ************************ ")

    var conn : Connection = null
    var tableIf : Table = null
    var list = List[Array[Byte]]()
    try {
      conn = ConnectionFactory.createConnection(getConfiguration(option))

      val rl = conn.getRegionLocator(TableName.valueOf(option.hTableName))

      for (key : Array[Byte] <- rl.getEndKeys) {
        if (key == null || key.length == 0) logWarning(option.hTableName+" : NULL or len is 0")
        else {
          list = List.concat(list, List(key))
        }
      }

      list
    } catch {
      case e: IOException => logError(e.getMessage, e); List()
    } finally {
      if (conn != null) try
        conn.close()
      catch {
        case e: IOException => logError(e.getMessage, e)
      }
    }
  }

  def getConfiguration(option: HBaseOption) : Configuration = {
    val conf = HBaseConfiguration.create
    conf
  }
}
