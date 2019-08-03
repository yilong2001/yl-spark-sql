package com.spark.dialect.sql.datasource.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.log4j.Logger

/**
  * Created by yilong on 2018/6/24.
  */
class HBaseConn {

}

object HBaseConn {
  var conn : Connection = null
  val logger = Logger.getLogger(classOf[HBaseConn])

  //TODO: connection pool
  def getConnection(conf : Configuration): Connection = {
    val isConnReady = (conn != null && !conn.isClosed && !conn.isAborted)

    isConnReady match {
      case true => conn
      case _ => {
        synchronized ({
          if ((conn != null && !conn.isClosed && !conn.isAborted)) conn
          else {
            if (conn != null) conn.close()
            try {
              conn = ConnectionFactory.createConnection(conf)
            } catch {
              case e : IOException => logger.error(e.getMessage, e); conn = null
            }

            conn
          }
        })
      }
    }
  }

}
