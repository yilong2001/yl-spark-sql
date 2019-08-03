package com.spark.dialect.sql.util

import org.apache.log4j.Logger

/**
  * Created by yilong on 2019/7/28.
  */
object CommonUtils {

  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    //val _logger = Logger.getLogger("CommonUtils::tryWithSafeFinally")
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          //_logger.warn(s"Suppressing exception in finally: ${t.getMessage}", t)
          throw originalThrowable
      }
    }
  }
}
