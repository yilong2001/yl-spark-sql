package com.spark.sql.examples.sql

import com.spark.dialect.sql.core.catalyst.analysis.ExtendSparkSqlParserV1
import com.spark.dialect.sql.core.catalyst.optimizer.{TrainModelIntoPathStrategy, UpdateSetOperationStrategy}
import com.spark.dialect.sql.core.catalyst.rules.UpdateSetOperationRules
import com.spark.dialect.sql.util.MetaConstants
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction

/**
  * Created by yilong on 2019/7/10.
  */
object AISql {
  def displayTimeCost(block: => Unit): Unit = {
    val starttime = System.currentTimeMillis()
    block
    val endtime = System.currentTimeMillis()
    println(s"${endtime}, ${starttime}, ${endtime-starttime}")
  }
  def getOldBalanceOrigValue(oldBalanceOrig: Double, newBalanceOrig: Double, amount: Double): Double = {
    if ((oldBalanceOrig == 0) && (newBalanceOrig == 0) && (amount != 0)) {
      -1.0
    } else {
      oldBalanceOrig
    }
  }

  def getNewBalanceOrigValue(oldBalanceOrig: Double, newBalanceOrig: Double, amount: Double): Double = {
    if ((oldBalanceOrig == 0) && (newBalanceOrig == 0) && (amount != 0)){
      -1.0
    }else{
      newBalanceOrig
    }
  }

  def getOldBalanceDestValue(oldBalanceDest: Double, newBalanceDest: Double, amount: Double): Double = {
    if ((oldBalanceDest == 0) && (newBalanceDest == 0) && (amount != 0)) {
      -1.0
    } else {
      oldBalanceDest
    }
  }

  def getNewBalanceDestValue(oldBalanceDest: Double, newBalanceDest: Double, amount: Double): Double = {
    if ((oldBalanceDest == 0) && (newBalanceDest == 0) && (amount != 0)) {
      -1.0
    } else {
      newBalanceDest
    }
  }

  def getTypeCode(x: String): Int = {
    if (x.equals("TRANSFER")) {
      0
    } else if (x.equals("CASH_OUT")) {
      1
    } else {
      1
    }
  }

  def main(args: Array[String]) : Unit = {
    type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    type RuleBuilder = SparkSession => Rule[LogicalPlan]
    type StrategyBuilder = SparkSession => Strategy

    val parserBuilder: ParserBuilder = (sparkSession, parser) => new ExtendSparkSqlParserV1(sparkSession, parser)
    val updateRuleBuilder: RuleBuilder = (sparkSession) => new UpdateSetOperationRules(sparkSession)
    val updateStrategyBuilder: StrategyBuilder = (sparkSession) => new UpdateSetOperationStrategy(sparkSession)
    val trainStrategyBuilder: StrategyBuilder = (sparkSession) => new TrainModelIntoPathStrategy(sparkSession)

    val extBuilder: ExtensionsBuilder = { e => {
        e.injectParser(parserBuilder)
        e.injectResolutionRule(updateRuleBuilder)
        e.injectPlannerStrategy(updateStrategyBuilder)
        e.injectPlannerStrategy(trainStrategyBuilder)
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
    conf.set("spark.sql.shuffle.partitions", "2")

    conf.set(MetaConstants.MEAT_DATA_TRAIN_MODEL_ALGORITHM_SERVERS, "localhost:29000")
    //conf.registerKryoClasses(Array(classOf[UpdateSetOperation],classOf[IndexedHashJoinExec]))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .withExtensions(extBuilder)
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("getOldBalanceOrigValue", AISql.getOldBalanceOrigValue _)
    spark.udf.register("getNewBalanceOrigValue", AISql.getNewBalanceOrigValue _)
    spark.udf.register("getOldBalanceDestValue", AISql.getOldBalanceDestValue _)
    spark.udf.register("getNewBalanceDestValue", AISql.getNewBalanceDestValue _)
    spark.udf.register("getTypeCode", AISql.getTypeCode _)

    val sQLContext = spark.sqlContext

    val p001 = "CREATE external TABLE IF NOT EXISTS p01 (name STRING, age INT,job STRING) " +
      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
      "TBLPROPERTIES ('header' = 'false', 'inferSchema' = 'false') " +
      "LOCATION '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/exdata' "
    //sQLContext.sql(p001)

    val p01 = "CREATE TEMPORARY VIEW p_part_log " +
      "(id INT, step INT,type STRING," +
      "amount DOUBLE,nameOrig STRING," +
      "oldbalanceOrg DOUBLE,newbalanceOrig DOUBLE," +
      "nameDest STRING,oldbalanceDest DOUBLE," +
      "newbalanceDest DOUBLE,isFraud INT," +
      "isFlaggedFraud INT) USING CSV OPTIONS " +
      "(sep ',' , header 'true' , path '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/source/part_log.csv') "
    sQLContext.sql(p01)

    val p02 =
      """
        |select step,
        | getTypeCode(type) as tcd,
        | amount,
        | getOldBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as oldbalanceOrg,
        |	getNewBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as newbalanceOrig,
        |	getOldBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as oldbalanceDest,
        |	getNewBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as newbalanceDest,
        | (newbalanceOrig+amount-oldbalanceOrg) as errorBalanceOrig,
        | (oldbalanceDest+amount-newbalanceDest) as errorBalanceDest,
        | isFraud as label
        | from p_part_log
        | where type = 'TRANSFER' or type = 'CASH_OUT'
      """.stripMargin

    //sQLContext.sql(p02).show()

    val p002 =
      """
        | CREATE external TABLE IF NOT EXISTS tmp_p_part_log (
        | step int,
        | tcd int,
        | amount double,
        | oldbalanceOrg double,
        | newbalanceOrig double,
        | oldbalanceDest double,
        | newbalanceDest double,
        | errorBalanceOrig double,
        | errorBalanceDest double,
        | label int
        | )
        | ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        | LOCATION '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/exdata'
      """.stripMargin
    sQLContext.sql(p002)
// TBLPROPERTIES ('header' = 'false', 'inferSchema' = 'false')

    // local directory '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/exdata'
    val p003 =
      """
        |insert overwrite table tmp_p_part_log
        |select step,
        | getTypeCode(type) as tcd,
        | amount,
        | getOldBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as oldbalanceOrg,
        |	getNewBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as newbalanceOrig,
        |	getOldBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as oldbalanceDest,
        |	getNewBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as newbalanceDest,
        | (newbalanceOrig+amount-oldbalanceOrg) as errorBalanceOrig,
        | (oldbalanceDest+amount-newbalanceDest) as errorBalanceDest,
        | isFraud as label
        | from p_part_log
        | where type = 'TRANSFER' or type = 'CASH_OUT'
      """.stripMargin
    //sQLContext.sql(p003).show()

    val train =
      """
        |TRAIN MODEL LogisticRegression FROM (
        |select step,
        | getTypeCode(type) as tcd,
        | amount,
        | getOldBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as oldbalanceOrg,
        |	getNewBalanceOrigValue(oldbalanceOrg,newbalanceOrig,amount) as newbalanceOrig,
        |	getOldBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as oldbalanceDest,
        |	getNewBalanceDestValue(oldbalanceDest,newbalanceDest,amount) as newbalanceDest,
        | (newbalanceOrig+amount-oldbalanceOrg) as errorBalanceOrig,
        | (oldbalanceDest+amount-newbalanceDest) as errorBalanceDest,
        | isFraud as label
        | from p_part_log
        | where type = 'TRANSFER' or type = 'CASH_OUT'
        |) p_part
        |INTO
        |'/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/models/lrmodel.m'
      """.stripMargin

    //displayTimeCost( sQLContext.sql(train).show() )

    val register =
      """
        | register model LogisticRegression
        | FROM '/Users/yilong/work/bigdata/code/mygithub/yl-spark-sql/datas/models/lrmodel.m'
        | AS lr_predict
      """.stripMargin

    displayTimeCost( sQLContext.sql(register).show() )

    val predict = "select label, " +
      "lr_predict(struct(step,tcd,amount,oldbalanceOrg,newbalanceOrig,oldbalanceDest,newbalanceDest,errorBalanceOrig,errorBalanceDest)) as predict from tmp_p_part_log limit 10"

    displayTimeCost( sQLContext.sql(predict).show() )

    //val udf1 = UserDefinedPythonFunction("m_add_one", )
  }
}
