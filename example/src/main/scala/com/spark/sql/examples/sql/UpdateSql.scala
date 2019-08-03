package com.spark.sql.examples.sql

import java.util.UUID

import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1
import com.spark.dialect.sql.core.catalyst.logical.UpdateSetOperation
import com.spark.dialect.sql.core.catalyst.optimizer.UpdateSetOperationStrategy
import com.spark.dialect.sql.core.catalyst.rules.UpdateSetOperationRules
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, ExprId}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.IndexedHashJoinExec
import org.apache.spark.sql.internal.SQLConf

/**
  * Created by yilong on 2019/6/4.
  */
object UpdateSql {
  def main(args: Array[String]) : Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    type RuleBuilder = SparkSession => Rule[LogicalPlan]
    type StrategyBuilder = SparkSession => Strategy

    val parserBuilder: ParserBuilder = (sparkSession, parser) => new ExtendSparkSqlParserV1(sparkSession, parser)
    val updateRuleBuilder:RuleBuilder = (sparkSession) => new UpdateSetOperationRules(sparkSession)
    val updateStrategyBuilder:StrategyBuilder = (sparkSession) => new UpdateSetOperationStrategy(sparkSession)

    val extBuilder: ExtensionsBuilder = { e => {
        e.injectParser(parserBuilder)
        e.injectResolutionRule(updateRuleBuilder)
        e.injectPlannerStrategy(updateStrategyBuilder)
      }
    }

    val warehouseLocation = "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/dw"

    val conf = new SparkConf()
    conf.setMaster("local[5]")
    conf.setAppName("SqlExample")
    conf.set("mapreduce.output.fileoutputformat.outputdir", "/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/test")
    conf.set("hive.exec.dynamic.partition", "true")
    conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    conf.set("spark.sql.warehouse.dir", warehouseLocation)
    //conf.set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    //conf.set("hive.metastore.uris", "thrift://localhost:9083")

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.broadcastTimeout", "1000")
    //conf.set("spark.default.parallelism", "6")
    conf.set("spark.sql.shuffle.partitions","2")
    //conf.registerKryoClasses(Array(classOf[UpdateSetOperation],classOf[IndexedHashJoinExec]))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .withExtensions(extBuilder)
      //.enableHiveSupport()
      .getOrCreate()

    val upsql = "UPDATE student SET age=tm.age FROM " +
      "(select a . r_id as aid, b.rid as bid from temp2 a, temp3 b where a.r_id = b.rid) tm " +
      "WHERE student.name = tm.name and student.id = tm.aid"
    val sQLContext = spark.sqlContext

    //sQLContext.sql(upsql)

    val upsql1 = "UPDATE student SET age=tm.age FROM " +
      " db.tm " +
      "WHERE student.name = tm.name and student.id = tm.aid"
    //sQLContext.sql(upsql1)

    val p010 = "CREATE TEMPORARY TABLE p01 (name STRING, age INT,job STRING) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false') " +
      "LOCATION '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/people.csv' "

    val p01 = "CREATE TEMPORARY VIEW p01 (name STRING, age INT,job STRING) USING CSV OPTIONS " +
      "(sep ',' , header 'false' , path '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/people.csv') "
    sQLContext.sql(p01)

    // create table from beeline
    val p0h =
      """
        |CREATE EXTERNAL TABLE p01 (name STRING, age INT,job STRING)
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        |LOCATION '/user/yilong/e_p01/'
        |TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');
      """.stripMargin


    val p02 = "CREATE TEMPORARY VIEW p02 (name STRING, age INT,job STRING) USING CSV OPTIONS " +
      "(sep ',' , header 'false' , path '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/people1.csv') "
    sQLContext.sql(p02)

    val p02h = """
                |CREATE EXTERNAL TABLE p02 (name STRING, age INT,job STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                |LOCATION '/user/yilong/e_p02/'
                |TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');
              """.stripMargin

    val p03 = "CREATE TEMPORARY VIEW p03 (name STRING, age INT,job STRING) USING CSV OPTIONS " +
      "(sep ',' , header 'false' , path '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/peopleup.csv') "
    sQLContext.sql(p03)

    val p03h = """
                |CREATE EXTERNAL TABLE p03 (name STRING, age INT,job STRING)
                |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                |LOCATION '/user/yilong/e_p03/'
                |TBLPROPERTIES ('header' = 'true', 'inferSchema' = 'false', 'skip.header.line.count' = '1');
              """.stripMargin

    sQLContext.sql("select * from p01").show()
    sQLContext.sql("select * from p02").show()
    sQLContext.sql("select * from p03").show()

    val u01 = "UPDATE p01 SET age = tm.age + 2, job = concat(tm.job,'_succ') FROM " +
      "(select p03.name as name1, p03.age as age, p03.job as job, p02.name as name2 " +
      " from p02 " +
      " inner join p03 " +
      " on p02.name = p03.name) tm " +
      " WHERE p01.name = tm.name1 and p01.name = concat(tm.name1,'') "

    val u02 = "UPDATE p01 SET age = p03.age + 2, job = concat(p03.job,'_good') FROM p03" +
      " WHERE p01.name = p03.name "

    val sqlResult = sQLContext.sql(u01)

    val u1 = UUID.randomUUID()
    val u2 = UUID.randomUUID()
    val eids = Seq(ExprId(22,u1), ExprId(0, u2))

    //System.out.println(eids.filter(x => x.equals(ExprId(22,u1))).nonEmpty)

    //sqlResult.explain()

    sqlResult.show()
  }
}
