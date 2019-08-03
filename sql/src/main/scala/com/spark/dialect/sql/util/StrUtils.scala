package com.spark.dialect.sql.util

import java.nio.ByteBuffer
import java.nio.charset.Charset

import org.apache.hadoop.hbase.util.Bytes.toBytes
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by yilong on 2018/7/16.
  */
object StrUtils {
  def avroUtf8ToString(obj : Any) : Any = {
    if (obj.isInstanceOf[org.apache.avro.util.Utf8]) {
      val u8 = obj.asInstanceOf[org.apache.avro.util.Utf8]
      new String(u8.getBytes)
    } else if (obj.isInstanceOf[String]) {
      obj.asInstanceOf[String]
    } else {
      obj
    }
  }

  def toUTF8String(obj : Any) : UTF8String = {
    val str : String = avroUtf8ToString(obj).toString

    val buffer = ByteBuffer.wrap(str.getBytes(Charset.forName("utf-8")))
    val offset = buffer.arrayOffset() + buffer.position()
    val numBytes = buffer.remaining()

    UTF8String.fromBytes(buffer.array(), offset, numBytes)
  }

  def strToAny(obj : String, dt : DataType) : Any = {
    if (obj == null) null
    else if (dt.isInstanceOf[StringType]) obj
    else if (dt.isInstanceOf[IntegerType]) obj.toInt
    else if (dt.isInstanceOf[LongType]) obj.toLong
    else if (dt.isInstanceOf[FloatType]) obj.toFloat
    else if (dt.isInstanceOf[DoubleType]) obj.toDouble
    else if (dt.isInstanceOf[BooleanType]) obj.toBoolean
    else obj
  }

  def splitString(org : String,  splitchar : String) : Array[String] = {
    if (org.contains(splitchar)) {
      val nsc = if (splitchar.equals(".")) "\\." else splitchar
      org.split(nsc)
    } else {
      Array[String](org)
    }
  }
}
