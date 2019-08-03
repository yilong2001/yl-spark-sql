package com.spark.dialect.sql.core.model

import com.spark.dialect.sql.util.RegMacro
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * Created by yilong on 2019/5/12.
  */
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression}

trait MatchedFilter {
  def getFilter : org.apache.spark.sql.sources.Filter
  def getAttr: String
  def getVal: Any
}

case class EqualMatchedFilter (val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class LessThanMatchedFilter(val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class LessThanOrEqualMatchedFilter( val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class GreatThanMatchedFilter( val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class GreatThanOrEqualMatchedFilter(val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class InMatchedFilter(val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class NotMatchedFilter(val filter:org.apache.spark.sql.sources.Filter, val attr:String, val v: Any)
  extends MatchedFilter{
  override def getFilter: org.apache.spark.sql.sources.Filter = filter
  override def getAttr: String = attr
  override def getVal: Any = v
}

case class ExpressionAttributeGroup(attrs:List[mutable.Queue[org.apache.spark.sql.catalyst.expressions.Attribute]],
exp:org.apache.spark.sql.catalyst.expressions.Expression)

case class ExpressionFilterGroup(filters:List[List[Option[MatchedFilter]]],
                                 expression:org.apache.spark.sql.catalyst.expressions.Expression) {
  def buildConstantMap() : Map[String, Any] = {
    val constantMap = new mutable.HashMap[String, Any]()
    filters.foreach(fl => {
      fl.foreach(f => {
        constantMap.put(f.get.getAttr, f.get.getVal)
      })
    })

    constantMap.toMap
  }
}

case class UDFMetaData(nickName : String, staticFuncName : String)

object MetaDatas {
  def udfMetasToString(udfMetas:Array[UDFMetaData]) : String = {
    udfMetas.map(udfmd => {
      "spark.udf.register(\""+udfmd.nickName+"\", "+udfmd.staticFuncName+" _)"
    }).mkString(";")
  }

  def registerUDF(spark: SparkSession, udfMetas:Array[UDFMetaData]) = {
    val regStr = udfMetasToString(udfMetas)
    println(regStr)
    //RegMacro(spark, regStr)
  }
}

