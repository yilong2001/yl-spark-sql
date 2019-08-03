package com.spark.dialect.sql.core.catalyst.analysis

import java.util.Locale

import com.spark.dialect.sql.core.catalyst.logical.{TrainAlgorithmType, TrainModelIntoPath, UpdateSetOperation}
import org.apache.spark.sql.catalyst.parser.{ParserInterface, ParserUtils}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.execution.SparkSqlAstBuilder
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.log4j.Logger
import com.spark.dialect.sql.core.execution.command.{HiveSnapshotBaseBuilder, RegisterModelFunctionCommand, UniSnapshot}
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseParser
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseParser._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseVisitor
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseVisitor._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseLexer
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseLexer._
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseListener
import com.spark.dialect.sql.core.catalyst.parser.SqlBaseBaseListener._
import com.spark.dialect.sql.core.exception.{AnalysisException, ParseException}
import com.spark.dialect.sql.util.StrUtils
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ParseTree
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction, UnresolvedRelation, UnresolvedStar, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, UnresolvedWindowExpression, WindowExpression}
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.Inner

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by yilong on 2019/1/16.
  */
class ExtendAstBuilderV1(val sparkSession : SparkSession,
                         val conf : SQLConf,
                         val parentParser : ParserInterface) extends SqlBaseBaseVisitor[AnyRef] {
  import ParserUtils._
  import scala.collection.JavaConversions._

  val logger = Logger.getLogger(classOf[ExtendAstBuilderV1])
  val hiveSnapshotCommandBuilder = HiveSnapshotBaseBuilder(sparkSession)

  override def visitMakeSnapshotCommand(ctx: MakeSnapshotCommandContext) : LogicalPlan = withOrigin(ctx) {
    val targetIdent = visitTableIdentifier(ctx.target)
    val sourceIdent = visitTableIdentifier(ctx.source)
    //val partitionKeys = Option(ctx.partitionSpec).map(visitNonOptionalPartitionSpec)
    val allPartitionKeys = ctx.partitionSpec().asScala.map(visitNonOptionalPartitionSpec)

    //Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)
    val isLocal = ctx.LOCAL != null
    val path : String = if (ctx.path != null) string(ctx.path) else null

    //hiveSnapshotCommandBuilder.build(sourceIdent, allPartitionKeys, targetIdent, isLocal, path)
    UniSnapshot.build(sparkSession, sourceIdent, allPartitionKeys, targetIdent, isLocal, path)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitTableIdentifier(ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  def visitStrictIdentifier(ctx: StrictIdentifierContext): UnresolvedAttribute = withOrigin(ctx) {
    UnresolvedAttribute(ctx.getText)
  }

  protected def visitNonOptionalPartitionSpec(ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).map {
      case (key, None) => throw new ParseException(s"Found an empty partition key '$key'.", ctx)
      case (key, Some(value)) => {
        logger.info(s"visitNonOptionalPartitionSpec : '$key' : '$value'")
        key -> (value)
      }
    }
  }

  override def visitPartitionSpec(ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val parts = ctx.partitionVal.map { pVal =>
      val name = pVal.identifier.getText
      val value = Option(pVal.constant).map(visitStringConstant)
      name -> value
    }
    // Before calling `toMap`, we check duplicated keys to avoid silently ignore partition values
    // in partition spec like PARTITION(a='1', b='2', a='3'). The real semantical check for
    // partition columns will be done in analyzer.
    checkDuplicateKeys(parts, ctx)
    parts.toMap
  }

  protected def visitStringConstant(ctx: ConstantContext): String = withOrigin(ctx) {
    ctx match {
      case s: StringLiteralContext => createString(s)
      case o => o.getText
    }
  }

  protected def createString(ctx: StringLiteralContext): String = {
    if (conf.escapedStringLiterals) {
      ctx.STRING().map(stringWithoutUnescape).mkString
    } else {
      ctx.STRING().map(string).mkString
    }
  }

  override def visitColValue(ctx: ColValueContext): Map[UnresolvedAttribute, Expression] = withOrigin(ctx) {
    val identify = visitStrictIdentifier(ctx.strictIdentifier())
    val ve = deanalyzeValueExpression(ctx.valueExpression())
    val pve = parentParser.parseExpression(ve)
    Map(identify -> pve) ++ Map()
  }

  override def visitColvaluelist(ctx: ColvaluelistContext): Map[UnresolvedAttribute, Expression] = withOrigin(ctx) {
    ctx.colValue().foldLeft(Map[UnresolvedAttribute, Expression]())((mp,cvc) => mp ++ visitColValue(cvc))
  }

  def visitParseTree(pt: org.antlr.v4.runtime.tree.ParseTree, sab:ArrayBuffer[String]) : Unit = {
    pt match {
//      case prc : org.antlr.v4.runtime.ParserRuleContext if prc.start.equals(prc.stop) => {
//        val tmp = prc
//        sab.append(tmp.getText)
//      }
//      case prc : org.antlr.v4.runtime.ParserRuleContext if !prc.start.equals(prc.stop) => {
//        val tmp = prc
//        prc.children.foreach(visitParseTree(_, sab))
//      }
//      case _ => None
      case tni : org.antlr.v4.runtime.tree.TerminalNodeImpl => {
        sab.append(tni.getText)
      }
      case tn : org.antlr.v4.runtime.tree.TerminalNode => {
        sab.append(tn.getText)
      }
      case _ => {
        val cc = pt.getChildCount;
        (0 to cc-1).foreach(id => {
          val child = pt.getChild(id)
          visitParseTree(child, sab)
        })
      }
    }
  }

  def deanalyzeFromClauseContext(fc : FromClauseContext) : String = {
    val sb = new ArrayBuffer[String]()
    fc.relation.foreach(rc => {
      rc.children.foreach(pt => {
        visitParseTree(pt, sb)
      })
    })

    val str = sb.mkString(" ")
    str
  }

  def deanalyzeWhereContext(wc : BooleanExpressionContext) : String = {
    val sb = new ArrayBuffer[String]()
    wc.children.foreach(pt => {
      visitParseTree(pt, sb)
    })

    val str = sb.mkString(" ")
    str
  }

  def deanalyzeValueExpression(vec : ValueExpressionContext) : String = {
    val sb = new ArrayBuffer[String]()
    if (vec.getChildCount == 0) {
      visitParseTree(vec, sb)
    } else {
      vec.children.foreach(pt => {
        visitParseTree(pt, sb)
      })
    }

    val str = sb.mkString(" ")
    str
  }

  override def visitUpdateSetFromCommand(ctx: UpdateSetFromCommandContext): LogicalPlan =  withOrigin(ctx) {
    val target = visitTableIdentifier(ctx.target)
    val sets = visitColvaluelist(ctx.sets).map(tp => {
      (new UnresolvedAttribute(Seq(target.identifier,tp._1.name)).asInstanceOf[org.apache.spark.sql.catalyst.expressions.Attribute], tp._2)
    })

    val where = parentParser.parseExpression(deanalyzeWhereContext(ctx.where))
    val from = deanalyzeFromClauseContext(ctx.fromClause())
    val select = this.parentParser.parsePlan("select * from "+from)
    val subselect = select match {
      case alias @ Project(_, SubqueryAlias(_,_)) => {
        if (alias.child.asInstanceOf[SubqueryAlias].name.identifier.contains("auto_generated_subquery_name")) {
          throw new IllegalArgumentException("{ "+from + " } clause must has alias name!")
        }
        alias.child
      }
      case ul @ Project(_, UnresolvedRelation(_)) => ul.child
      case _ => throw new IllegalArgumentException("{ "+from + " } is error!")
    }

    val subSelectRelationName = subselect match {
      case alias : SubqueryAlias => {
        alias.name.identifier
      }
      case rl : UnresolvedRelation => rl.tableIdentifier.identifier
      case _ => throw new IllegalArgumentException("{ "+from + " } is error!")
    }

    val left = UnresolvedRelation(target)

    //TODO:
    def renameAttribute(attr : org.apache.spark.sql.catalyst.expressions.Attribute) : String = {
      attr.name + "__t"
    }

    def renameExpression(exp : Expression) : Expression = {
      val tssrn1 = StrUtils.splitString(subSelectRelationName,".")

      exp transformUp {
        case attribute : org.apache.spark.sql.catalyst.expressions.Attribute => {
          val t1 = StrUtils.splitString(attribute.name.toString, ".")
          val t2 = t1.slice(0, t1.length - 1)
          if (t2.length > tssrn1.length) {
            attribute
          } else {
            tssrn1.slice(tssrn1.length - t2.length, tssrn1.length).zip(t2).forall(tp => {
              if (tp._1.equals(tp._2)) { true } else { false }
            }) match {
              case true => UnresolvedAttribute(renameAttribute(attribute))
              case _ => attribute
            }
          }
        }
      }
    }

    val right = subselect

    //TODO: NestedLoopsJoin
    val join = Join(left, subselect, Inner, Some(where))

    val renamedSets = sets.map(tp => {
      (tp._1, renameExpression(tp._2))
    })

    logger.info("******************* (1) ****************************")
    logger.info(sets)
    logger.info(renamedSets)

    logger.info(target)
    logger.info(where)
    logger.info(select)
    logger.info(subselect)
    logger.info(left)
    logger.info(join)
    logger.info("******************** (2) ***************************")
    logger.info(join.output)
    logger.info(left.output)
    logger.info("********************* (3) **************************")

    UpdateSetOperation(Seq.empty, join, target, sets, Some(where), false)
  }

  def visitTrainLogisticRegressionCommand(fromPlan: LogicalPlan, path: String): LogicalPlan = {
    TrainModelIntoPath(fromPlan.output, fromPlan, path, TrainAlgorithmType.LR)
  }

  def parseFromClauseFromSelect(from : String) : LogicalPlan = {
    val select = this.parentParser.parsePlan("select * from "+from)
    val subselect = select match {
      case alias @ Project(_, SubqueryAlias(_,_)) => {
        if (alias.child.asInstanceOf[SubqueryAlias].name.identifier.contains("auto_generated_subquery_name")) {
          throw new IllegalArgumentException("{ "+from + " } clause must has alias name!")
        }
        alias.child
      }
      case ul @ Project(_, UnresolvedRelation(_)) => ul.child
      case _ => throw new IllegalArgumentException("{ "+from + " } is error!")
    }

    subselect
  }

  def getRelationNameFromSubSelect(subselect : LogicalPlan) : String = {
    val subSelectRelationName = subselect match {
      case alias : SubqueryAlias => {
        alias.name.identifier
      }
      case rl : UnresolvedRelation => rl.tableIdentifier.identifier
      case _ => throw new IllegalArgumentException(s" ${subselect} is error!")
    }

    subSelectRelationName
  }

  override def visitTrainModelFromCommand(ctx: TrainModelFromCommandContext): LogicalPlan =  withOrigin(ctx) {
    val targetModel = ctx.target.getText
    val from = deanalyzeFromClauseContext(ctx.fromClause())

    val fromPlan = parseFromClauseFromSelect(from)
    val subSelectRelationName = getRelationNameFromSubSelect(fromPlan)
    val path : String = string(ctx.path)

    targetModel match {
      case "LogisticRegression" => visitTrainLogisticRegressionCommand(fromPlan, path)
      case _ => throw new IllegalArgumentException(s"not support ${targetModel}, only support LogisticRegression | ")
    }
  }

  override def visitRegisterModeFunctionCommand(ctx: RegisterModeFunctionCommandContext): LogicalPlan = withOrigin(ctx) {
    val model = ctx.target.getText
    val filepath = string(ctx.path)
    val funcname = ctx.name.getText

    RegisterModelFunctionCommand(model, filepath, funcname)
  }
}
