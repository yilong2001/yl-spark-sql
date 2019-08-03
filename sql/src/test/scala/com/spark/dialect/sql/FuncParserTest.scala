package com.spark.dialect.sql

import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1
import com.spark.dialect.sql.core.catalyst.rules.{ExpressionTransformHelper, ReplaceAttributeExp, ResolveAttributeExp, ResolveFunctionExp}
import com.spark.dialect.sql.util.ExpressionUtils._
import com.spark.dialect.sql.util.{ExpressionEvalHelper, SqlFilterUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by yilong on 2019/5/16.
  */


object FuncParserTest {
  def test0(spark:SparkSession, parser : ExtendSparkSqlParserV1) : Unit = {
    val func1 = "concat(reverse(`f.id`),'_',substr(name,0,2),pow(2,grade+1))"
    val func5 = "concat(reverse(id),'_',9999999999999 - unix_timestamp(concat(dt,substring(concat('000000',tm), -6)), 'yyyyMMddHHmmss') * 1, '_', num,'_',no)"
    val func2 = "`f.name`"
    val func3 = "pow(grade,grade)"

    val schema = new StructType()
      .add("id", StringType)
      .add("dt", StringType)
      .add("tm", StringType)
      .add("num", IntegerType)
      .add("no", StringType)

    val constantMap = new mutable.HashMap[String, Any]()
    constantMap.put("id", "908173916231")
    constantMap.put("dt", "219052401")
    constantMap.put("tm", "101")
    constantMap.put("num", 2)
    constantMap.put("no", "abc")

    val ot = parser.parseExpression(func5)
    val pot = ResolveAttributeExp(schema)(ot)

    val s1 = System.currentTimeMillis()
    val pot1 = ResolveFunctionExp(spark,parser.sQLConf)(ReplaceAttributeExp(constantMap.toMap, schema, false)(pot))
    //(0 to 10).foreach(x => {
    //  val rout = ExpressionEvalHelper.evaluateWithGeneratedMutableProjection(pot)
    //  System.out.println(rout) //.asInstanceOf[InternalRow]
    //})
    System.out.println(System.currentTimeMillis() - s1)
    //System.exit(1)

    System.out.println("**********************")
    //buf.foreach(x=>System.out.println(resolveFunction(spark, x)))

    val tmp = resolveRowkeyFunction(parser, func5, schema)

    val f1 = org.apache.spark.sql.sources.And(
      org.apache.spark.sql.sources.And(EqualTo(("id"), "908173916231"),LessThan(("dt"), "20190601"))
      , org.apache.spark.sql.sources.GreaterThan(("mt"), "20190601"))

    val f2 = org.apache.spark.sql.sources.Or(EqualTo(("`f.id`"), "100"),
      EqualTo(("name"), "hi"))

    val f3 = EqualTo(("`f.id`"), "100")

    val filters: Array[Filter] = Array(f3)
    val filtersQueue = new mutable.Queue[ArrayBuffer[Filter]]

    filters.foreach(x => filtersQueue.enqueue(ArrayBuffer(x)))
    //tmp.values()
  }

  def test1(spark:SparkSession, parser : ExtendSparkSqlParserV1) : Unit = {
    val func5 = "concat(reverse(id),'_',119999999999 - unix_timestamp(concat(dt,substring(concat('000000',tm), -6)), 'yyyyMMddHHmmss') * 1, '_', num,'_',no)"

    val schema = new StructType()
      .add("id", StringType)
      .add("dt", StringType)
      .add("tm", StringType)
      .add("num", IntegerType)
      .add("no", StringType)

    val constantMap = new java.util.HashMap[String, Any]()
    constantMap.put("id", "908173916231")
    constantMap.put("dt", "219052401")
    constantMap.put("tm", "101")
    constantMap.put("num", 2)
    constantMap.put("no", "abc")

    val eags = resolveRowkeyFunction(parser, func5, schema)

    val filter1 = org.apache.spark.sql.sources.And(
      org.apache.spark.sql.sources.And(EqualTo(("id"), "908173916231"),EqualTo(("dt"), "20190601"))
      , org.apache.spark.sql.sources.EqualTo(("tm"), "20190601"))

    val compiledFilters = SqlFilterUtils.compileSqlFilter(filter1)

    val cf1 = compiledFilters.apply(0)

    val mexp = matchCutoffedExppression(cf1, eags)

    val repa = ReplaceAttributeExp(mexp.buildConstantMap(), schema, false)(mexp.expression)
    //val pot = resolveFunction(spark, repa)
    var potX = ExpressionTransformHelper.resolveExpression(spark, parser.sQLConf)(repa)

    System.out.println(potX)

    val f6 = "concat(reverse('12345'),'_',19999999999999 - unix_timestamp(concat('20190526',substring(concat('000000','101'), -6)), 'yyyyMMddHHmmss') * 1)"
    val ot = parser.parseExpression(f6)
    //val pot2 = resolveFunction(spark, ot)
    val rout = ExpressionEvalHelper.evaluateWithGeneratedMutableProjection(potX)

    spark.sparkContext.getConf
    System.out.println(rout)
  }

  def main(args: Array[String]) : Unit = {
    val warehouseLocation = "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/dw"

    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("SqlExample")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/test")
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("spark.sql.warehouse.dir", warehouseLocation)
    conf.set("hive.metastore.uris", "thrift://localhost:9083")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate()

    var rkinfo = ""

    //(0 to 0).foreach(x=>{
    //  val rk = spark.sql("select concat(reverse('9876543'),'_',9999999999 - unix_timestamp(concat('20190526',substring(concat('000000','101'), -6)), 'yyyyMMddHHmmss') * 1000)").collect().apply(0).get(0)
    //  val cd = "select concat(reverse(\"001\")) from }"
      //RegMacro(spark, cd)
    //  System.out.println(s"rkinfo = ${rkinfo}; rk = ${rk}")
    //})
    val parser = new ExtendSparkSqlParserV1(spark, new SparkSqlParser(new SQLConf))

    test1(spark, parser)
  }
}

/**
  *

  r = funcN1(funcN2(n4, n5, funcN6(n10, n11)), funcN3(n7, n8, n9))

                              n1
                n2                           n3
         n4     n5     n6             n7     n8     n9
                     n10 n11
  step 1: del n9 ==> funcN1(funcN2(n4, n5, funcN6(n10, n11)), funcN3(n7, n8))
  step 2: del n8 n9 ==> funcN1(funcN2(n4, n5, funcN6(n10, n11)), funcN3(n7))
  step 3: del n3 n7 n8 n9 ==> funcN1(funcN2(n4, n5, funcN6(n10, n11)))
  step 4: del n11 n3 n7 n8 n9 ==> funcN1(funcN2(n4, n5, funcN6(n10)))
  step 5: del n6 n10 n11 n3 n7 n8 n9 ==> funcN1(funcN2(n4, n5))
  step 6: del n5 n6 n10 n11 n3 n7 n8 n9 ==> funcN1(funcN2(n4))
  **/