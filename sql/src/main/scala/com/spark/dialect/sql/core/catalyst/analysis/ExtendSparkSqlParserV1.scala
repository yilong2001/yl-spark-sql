package com.spark.dialect.sql.core.catalyst.analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, ParseErrorListener, ParserInterface}
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseParser
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseParser._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseVisitor
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseVisitor._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseLexer
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseLexer._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseListener
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseListener._
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.{CodegenObjectFactoryMode, Expression}
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.{DataType, StructType}

/**
  * Created by yilong on 2019/1/16.
  */
class ExtendSparkSqlParserV1(val sparkSession : SparkSession,
                             val parentParser: ParserInterface) extends ExtendAbstractSqlParserV1 {
  val sQLConf = new SQLConf()

  //sQLConf.setConfString(SQLConf.CODEGEN_FACTORY_MODE.key,
  //  CodegenObjectFactoryMode.NO_CODEGEN.toString)

  sparkSession.sparkContext.getConf.getAll.foreach { case (k, v) =>
    sQLConf.setConfString(k, v)
  }

  val astBuilder = new ExtendAstBuilderV1(sparkSession, sQLConf, parentParser) //sparkSession.sqlContext.conf

  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) {
    parser => {
      try {
        val stat = parser.singleStatement()
        val res = astBuilder.visitSingleStatement(stat)
        //astBuilder.visitSingleStatement(parser.singleStatement()) match {
        res match {
          case plan: LogicalPlan => plan
          case _ => {
            val p = parentParser.parsePlan(sqlText)
            p
          }
        }
      } catch {
        case e: Exception => {
          logger.error(e)
          parentParser.parsePlan(sqlText)
        }
      }
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    logger.info("parseExpression : " + sqlText)
    return parentParser.parseExpression(sqlText)
  }

  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    logger.info("parseTableIdentifier : " + sqlText)
    return parentParser.parseTableIdentifier(sqlText)
  }

  override def parseTableSchema(sqlText: String): StructType = {
    logger.info("parseTableSchema : " + sqlText)
    return parentParser.parseTableSchema(sqlText)
  }

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    logger.info("parseFunctionIdentifier : " + sqlText)
    return parentParser.parseFunctionIdentifier(sqlText)
  }

  override def parseDataType(sqlText: String): DataType = {
    logger.info("parseDataType : " + sqlText)
    return parentParser.parseDataType(sqlText)
  }
}

