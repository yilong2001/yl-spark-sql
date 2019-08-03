package com.spark.dialect.sql.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Created by yilong on 2019/7/31.
  */
object SerdeUtils {
  def write[A](o: A): Array[Byte] = {
    val ba = new ByteArrayOutputStream(1024)
    val out = new ObjectOutputStream(ba)
    out.writeObject(o)
    out.close()
    ba.toByteArray()
  }

  def read[A](buffer: Array[Byte]): A = {
    val in = new ObjectInputStream(new ByteArrayInputStream(buffer))
    in.readObject().asInstanceOf[A]
  }
}
