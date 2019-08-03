package com.spark.dialect.sql.util

import org.apache.spark.sql.sources.{And, Filter, Or}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
  * Created by yilong on 2019/5/21.
  */
object SqlFilterUtils {
  def isComposeFilter(f : Filter) : Boolean = {
    f match {
      case Or(_, _) => true
      case And(_, _) => true
      case _ => false
    }
  }

  def compileSqlFilter(filter:Filter) : mutable.Queue[ArrayBuffer[Filter]] = {
    val rawFilters = new mutable.Queue[ArrayBuffer[Filter]]
    val compiledFilters = new mutable.Queue[ArrayBuffer[Filter]]
    Array(filter).foreach(x => rawFilters.enqueue(ArrayBuffer(x)))

    doCompileSqlFilters(rawFilters, compiledFilters)

    compiledFilters
  }

  def doCompileSqlFilters(filtersQueueIn: mutable.Queue[ArrayBuffer[Filter]],
                             filtersQueueOut: mutable.Queue[ArrayBuffer[Filter]]) : Unit = {
    val pendingQueue = new mutable.Queue[ArrayBuffer[Filter]]()

    filtersQueueIn.foreach(tt => {
      var isCompFilterFlag = false
      val cur = tt
      val loop = new Breaks
      loop.breakable {
        cur.foreach(t => {
          if (isComposeFilter(t)) {
            pendingQueue.enqueue(cur)
            isCompFilterFlag = true
            loop.break()
          }
        })
      }

      if (!isCompFilterFlag) {
        filtersQueueOut.enqueue(cur)
      }
    })

    if (pendingQueue.size == 0) {
      // nothing to do for compose filter, and return
      return
    }

    filtersQueueIn.clear()

    pendingQueue.foreach(tt => {
      var fs : ArrayBuffer[Filter] = tt

      var f0 : Filter = null
      var f01 : Filter = null
      var f02 : Filter = null

      val loop = new Breaks;
      var isReplicated = false
      loop.breakable {
        fs.foreach(t => {
          t match {
            case Or(f1, f2) => {
              isReplicated = true
              f0 = t; f01 = f1; f02 = f2;
              loop.break()
            }
            case And(f1, f2) => {
              f0 = t;f01 = f1;f02 = f2;
              loop.break()
            }
            case _ =>
          }
        })
      }

      if (f0 == null || f01 == null || f02 == null) {
        throw new IllegalArgumentException(" compileFilter has unknown exception : f == null || f01 == null || f02 == null ")
      }

      fs = fs.filter(f => !f.equals(f0))

      if (isReplicated) {
        val newfs = new ArrayBuffer[Filter]()
        fs.foreach(t => {
          newfs.append(t)
        })

        fs.append(f01)
        newfs.append(f02)

        filtersQueueIn.enqueue(newfs)
      } else {
        fs.append(f01)
        fs.append(f02)
      }

      filtersQueueIn.enqueue(fs)
    })

    doCompileSqlFilters(filtersQueueIn, filtersQueueOut)
  }

}
