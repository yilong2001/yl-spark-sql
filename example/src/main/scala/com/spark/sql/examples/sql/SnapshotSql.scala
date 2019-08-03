package com.spark.sql.examples.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.TableIdentifier

import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1

/**
  * Created by yilong on 2018/12/21.
  */


object SnapshotSql {
  def main(args: Array[String]) : Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    val parserBuilder: ParserBuilder = (sparkSession, parser) => new ExtendSparkSqlParserV1(sparkSession,parser)
    val extBuilder: ExtensionsBuilder = { e => e.injectParser(parserBuilder)}

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
      .withExtensions(extBuilder)
      .enableHiveSupport()
      .getOrCreate()

    //val str = "select * from pp1"
    //val str = "MAKE SNAPSHOT snapshot_pp1 FROM pp1"
    //val parser = new SparkSqlParser(new SQLConf)//
    //val parser = new ExtendSparkSqlParserV1(spark, null)
    //val logicPlan = parser.parsePlan(str)
    //println(logicPlan.toJSON)

    val sQLContext = spark.sqlContext
    //spark.sparkContext

    val p0 = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/people.csv")

    val p1 = spark.read.format("csv")
      .option("sep", ";")
      //.option("inferSchema", "true")
      .option("header", "true")
      .load("/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/testdata")

    //val p2 = spark.sql("select * from csv.`/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/testdata/people.csv`")
    val p2 = spark.sql("CREATE TEMPORARY VIEW pp1 (name STRING, age INT,job STRING) USING CSV OPTIONS " +
      "(sep ';' , header 'true' , path '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/people1.csv') ")
    //  "('sep'=',','header'='true','inferSchema'='true') " +
    //  "LOCATION `/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/testdata/people.csv`")
    //val p3 = spark.sql("select * from pp1")
    //p3.show()

    //p0.createOrReplaceTempView("people")
    //p1.createOrReplaceTempView("people1")

    // SQL can be run over a temporary view created using DataFrames
    //val results = sQLContext.sql("SELECT name FROM people")
    //results.show()
    //results.explain(true)

    //val results2 = sQLContext.sql("from people insert into people1 select *")
    //results2.explain(false)

    //val result3 = sQLContext.sql("create table p2 like people")
    //result3.explain(true)
    //val sql4 = "CREATE TABLE p_p2 ( name string, job string ) PARTITIONED BY (age int)";
    //val result4 = sQLContext.sql(sql4)
    //result4.explain(true)

    //val result5 = sQLContext.sql("create table p_p3 like p_p2")
    //val result61 = sQLContext.sql("insert into p_p2 partition (age) select name, job, age from people1 ")
    //val result62 = sQLContext.sql("insert into p_p3 partition (age) select name, job, age from people1 ")

    val sql8 = "CREATE TABLE p_p82 ( name string, job string ) PARTITIONED BY (age int, sex string)";
    //val result81 = sQLContext.sql(sql8)
    //result4.explain(true)

    //val result82 = sQLContext.sql("create table p_p83 like p_p82")

    //val result83 = sQLContext.sql("insert into p_p82 partition (age=10,sex='M') select name, job from people1 ")
    //val result84 = sQLContext.sql("insert into p_p83 partition (age,sex) select name, job, age, 'F' as sex from people1 p")

    //result5.explain(true)

    val catalog = spark.sessionState.catalog
    //TODO: catalog 暂时不考虑
//    val sourceTableDesc1 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("pp1"))
//    println(sourceTableDesc1)
//
//    val sourceTableDesc2 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("p2"))
//    println(sourceTableDesc2)
//
//    val sourceTableDesc3 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("people"))
//    println(sourceTableDesc3)
//
//    val sourceTableDesc4 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("people1"))
//    println(sourceTableDesc4)
//
//    val sourceTableDesc5 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("p_p2"))
//    println(sourceTableDesc5)

    //val sourceTableDesc6 = catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("p_p3"))
    //println(sourceTableDesc6)

    //val conf1 = new SparkConf
    //conf1.set("testkey","testval")

    //def fortest(conf: SparkConf): String = {
    //  conf.get("testkey") match {
    //    case "hive" => "forhive"
    //    case "in-memory" => "forinmem"
    //  }
    //}

    //println(fortest(conf1))

    //val result51 = sQLContext.sql("MAKE SNAPSHOT snapshot_pp83 FROM p_p83")
    //val result511 = sQLContext.sql("MAKE SNAPSHOT snapshot_pp83 " +
    //  " DIRECTORY '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datasource' " +
    //  " FROM p_p83 " +
    //  " PARTITION (age=21, sex='F') PARTITION (age=25, sex='M')")

    //val result52 = sQLContext.sql("MAKE SNAPSHOT snapshot_pp83 DIRECTORY '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/output' FROM p_p83")
    //val result53 = sQLContext.sql("MAKE SNAPSHOT snapshot_pp83 LOCAL DIRECTORY '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/output' FROM p_p83")

    val result61s = """
        |create external table hbase_myrecord_t1(rowkey STRING,uid STRING,nick STRING,grade INT)
        |STORED BY 'org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler'
        |WITH SERDEPROPERTIES( "hbase.columns.mapping"=":key,f:uic,f:nick,f:grade")
        |TBLPROPERTIES("spark.dialect.hbase.table.name"="myrecord_t1",
        |"spark.dialect.hbase.zookeeper.quorum"="localhost",
        |"spark.dialect.hbase.zookeeper.property.clientPort"="2181")
      """.stripMargin

    val result62s = "make snapshot myrecord_t1_snapshot from hbase_myrecord_t1"

    //sQLContext.sql(result61s)
    //sQLContext.sql(result62s)

    val result71s =
      """
        |insert into hbase_myrecord_t1 select 'uid_a100001_nick_a100001'
        |as rowkey, 'uid_a100001' as uid, 91 as grade from demo_parquet1
      """.stripMargin

    sQLContext.sql(result71s)
  }
}
