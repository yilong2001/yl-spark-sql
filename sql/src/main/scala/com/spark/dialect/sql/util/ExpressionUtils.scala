package com.spark.dialect.sql.util

import java.util

import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1
import com.spark.dialect.sql.core.catalyst.rules.ResolveAttributeExp
import com.spark.dialect.sql.core.model._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, ComplexTypeMergingExpression, Expression, NamedExpression, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Stack}
import scala.util.control.Breaks

/**
  * Created by yilong on 2019/5/20.
  */

object ExpressionUtils {
  def findAttribute(exp:Expression, queue: mutable.Queue[org.apache.spark.sql.catalyst.expressions.Attribute]) : Unit = {
    exp match {
      case attr @ UnresolvedAttribute(nameParts) => {
        queue.enqueue(attr)
      }
      case ar @ AttributeReference(name,dataType,nullable,metadata) => {
        queue.enqueue(ar)
      }
      case e : org.apache.spark.sql.catalyst.expressions.Expression if e.children.size == 0 => {

      }
      case e : org.apache.spark.sql.catalyst.expressions.Expression => {
        e.children.foreach(findAttribute(_, queue))
      }
      case _ => {}
    }
  }

  def nextChildSlice(id:Int,len:Int, child:Seq[Expression]) : Seq[Expression] = {
    if (id < len-1) {
      child.slice(id+1, len)
    } else {
      List()
    }
  }

  def cutoffExpression(spark:SparkSession,
                       exp:Expression,
                       buf:ArrayBuffer[Expression]): Unit = {
    System.out.println(exp)
    buf += exp
    exp match {
      case UnresolvedFunction(name,children,isDistinct) => {
        val childs = children.toList.reverse
        val len = childs.size

        childs.zipWithIndex.foreach(tuple=> {
          val next = nextChildSlice(tuple._2,len,childs)
          if (next.size > 0) {
            buf += UnresolvedFunction(name, next.reverse, isDistinct)
          }
        })
      }
    }
  }

  def resolveRowkeyFunction(parser: ExtendSparkSqlParserV1,
                            func: String,
                            schema: StructType) :
  List[ExpressionAttributeGroup] = {
    import scala.collection.mutable.ArrayBuffer

    val buf = new ArrayBuffer[Expression]()
    val out = parser.parseExpression(func)

    //resolve attribute for UnresolvedAttribute
    val resolveOut = ResolveAttributeExp(schema)(out)//out//resolveFunction(resolveAttribute(out))

    //cutoff expression into independent parts
    cutoffExpression(parser.sparkSession, resolveOut, buf)

    //filter illegeal function after cutoff expression
    //TODO: for simple scene: concat(xxx), it is not necessary
//    val nbuf = buf.filter(p => {
//      try {
//        resolveFunction(parser.sparkSession, p)
//        true
//      } catch {
//        case e:Exception => false
//      }
//    })

    val attrBuf = new ArrayBuffer[mutable.Queue[org.apache.spark.sql.catalyst.expressions.Attribute]]()

    val allAttrExp = buf.map(exp => {
      val q = new mutable.Queue[org.apache.spark.sql.catalyst.expressions.Attribute]
      findAttribute(exp, q)
      (q,exp)
    }).map(x=>{
      (x._1.toList.map(ua => ua.name.replace("`","")).mkString(","), x)
    })

    //to distinct, for same attrs with different exp parts, the shortest exp parts will be selected
    val tmap = new mutable.HashMap[String,(mutable.Queue[org.apache.spark.sql.catalyst.expressions.Attribute], Expression)]()

    //do distinct
    allAttrExp.toList.reverse.foreach(t => {
      if (!tmap.isDefinedAt(t._1)) {
        tmap.put(t._1, t._2)
      }
    })

    tmap.values.toList.reverse.reduce((tp1,tp2) => {
      if (attrBuf.size == 0) {
        attrBuf += tp1._1
      }

      attrBuf += (tp2._1.slice(tp1._1.size, tp2._1.size))

      tp2
    })

    val eags = tmap.values.toList.reverse.zipWithIndex.map(tp => {
      ExpressionAttributeGroup(attrBuf.toList.slice(0,tp._2+1), tp._1._2)
    })

    eags.reverse
  }

  import org.apache.spark.sql.sources.{And, Filter, Or}

  def matchCutoffedExppression(filters:ArrayBuffer[Filter],
      cols : List[ExpressionAttributeGroup]) : ExpressionFilterGroup = {
    val filterMap = new mutable.HashMap[String,Filter]
    filters.foreach(f => {
      filterMap.put(f.references.mkString(","), f)
    })

    val fmes = cols.map(eag => {
      val e = eag.exp
      val mapFilters = eag.attrs.map(aq => {

        val aql = aq.map(a => {
          filterMap.get(a.name.replace("`","")).map(f => {
            f match {
              case EqualTo(col, v) => EqualMatchedFilter(f,col,v)
              case EqualNullSafe(col, v) => EqualMatchedFilter(f,col,v)
              case LessThan(col, v: Int) => LessThanMatchedFilter(f,col,v)
              case LessThanOrEqual(col, v: Int) => LessThanOrEqualMatchedFilter(f,col,v)
              case GreaterThan(col, v: Int) => GreatThanMatchedFilter(f,col,v)
              case GreaterThanOrEqual(col, v: Int) => GreatThanOrEqualMatchedFilter(f,col,v)
              case In(col, values) => throw new IllegalArgumentException(s"${f} is not supported")
              case IsNull(col) => throw new IllegalArgumentException(s"${f} is not supported")
              case IsNotNull(col) => throw new IllegalArgumentException(s"${f} is not supported")
              case Not(pred) => throw new IllegalArgumentException(s"${f} is not supported")
              case And(left, right) => throw new IllegalArgumentException(s"${f} is not supported")
              case Or(left, right) => throw new IllegalArgumentException(s"${f} is not supported")
              case _ => throw new IllegalArgumentException(s"${f} is not supported")
            }
          })
        })

        aql.toList
      })

      ExpressionFilterGroup(mapFilters, e)
    })

    val fmes1 = fmes.filter(p => {
      var noneCnt = 0
      p.filters.foreach(pl1 => {
        pl1.foreach(p2 => {
          if (p2 == None) noneCnt = noneCnt+1
        })
      })

      (noneCnt == 0)
    })

    val fmes2 = fmes1.filter(p => {
      var notEqualCnt = 0
      val pslice = p.filters.slice(0, p.filters.size - 1)
      p.filters.foreach(pl1 => {
        pl1.foreach(p2 => {
          p2 match {
            case Some(EqualMatchedFilter(_,_,_)) => {}
            case _ => notEqualCnt = notEqualCnt + 1
          }
        })
      })
      notEqualCnt == 0
    })

    (fmes2.size > 0) match {
      case true => fmes2.apply(0)
      case _ => null
    }
  }
}
