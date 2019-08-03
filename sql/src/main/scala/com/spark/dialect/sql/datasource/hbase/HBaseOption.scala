package com.spark.dialect.sql.datasource.hbase

import java.sql.Connection
import java.util.{Locale, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
  * Created by yilong on 2019/5/12.
  */

class HBaseOption(val hTableName : String,
                  @transient private val parameters: CaseInsensitiveMap[String],
                  @transient private val h2tCols : Map[String, String],
                  @transient private val t2hCols : Map[String, String])
  extends Serializable {
  import HBaseOption._

  //(1) hbase config; (2) hbase cols to table cols; (3) table cols to hbase cols
  def this(tableName: String,
           parameters: Map[String, String],
           h2tCols : Map[String, String],
           t2hCols : Map[String, String]) = {
    this(tableName, CaseInsensitiveMap(parameters), h2tCols, t2hCols)
  }

  /**
    * Returns a property with all parameters.
    */
  val asProperties: Properties = {
    val properties = new Properties()
    parameters.originalMap.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  //hbase to table cols map
  val asH2TCols: Properties = {
    val properties = new Properties()
    h2tCols.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  //table to hbase cols map
  val asT2HCols: Properties = {
    val properties = new Properties()
    t2hCols.foreach { case (k, v) => properties.setProperty(k, v) }
    properties
  }

  /**
    * Returns a property with all options except Spark internal data source options like `url`,
    * `dbtable`, and `numPartition`. This should be used when invoking JDBC API like `Driver.connect`
    * because each DBMS vendor has its own property list for JDBC driver. See SPARK-17776.
    */
  val asHBaseConfig: Configuration = {
    val conf = new Configuration
    conf.setClassLoader(classOf[HBaseConfiguration].getClassLoader)

    parameters.originalMap.foreach {case (k,v) => conf.set(k, v)}

    HBaseConfiguration.create(conf)
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  require(parameters.isDefinedAt(HBASE_TABLE_NAME), s"Option '$HBASE_TABLE_NAME' is required.")
  require(parameters.isDefinedAt(HBASE_ZK_QUORM), s"Option '$HBASE_ZK_QUORM' is required.")
  require(parameters.isDefinedAt(HBASE_ZK_QUORM_CLIENT_PORT), s"Option '$HBASE_ZK_QUORM_CLIENT_PORT' is required.")

  //require(parameters.isDefinedAt(HBASE_TABLE_ROWKEY_TRANSFER), s"Option '$HBASE_TABLE_ROWKEY_TRANSFER' is required.")

  //val keyClassName = parameters.getOrElse(HBASE_TABLE_KEY_CLASS_NAME, "java.lang.String")
  //val valueClassName = parameters(HBASE_TABLE_VALUE_CLASS_NAME)

  //val valueClass = ClassUtilInScala.classForName(valueClassName)

  // ------------------------------------------------------------
  // Optional parameters only for writing
  // TODO: don't support create/drop table
  // ------------------------------------------------------------
  // TODO: to reuse the existing partition parameters for those partition specific options
  val isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED
}


object HBaseOption {
  private val hbaseOptionNames = collection.mutable.Set[String]()

  private def newOption(name: String): String = {
    hbaseOptionNames += name.toLowerCase(Locale.ROOT)
    name
  }

  val HBASE_TABLE_NAME = newOption("tableName")

  val HBASE_ZK_QUORM = newOption("hbase.zookeeper.quorum")
  val HBASE_ZK_QUORM_CLIENT_PORT = newOption("hbase.zookeeper.property.clientPort")
}

