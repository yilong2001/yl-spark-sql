package com.spark.dialect.sql.core.exception

import org.antlr.v4.runtime.ParserRuleContext
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.trees.Origin

/**
  * Created by yilong on 2019/1/17.
  */
class ParseException(val command: Option[String],
                     message: String,
                     val start: Origin,
                     val stop: Origin) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(ParserUtils.command(ctx)),
      message,
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    new ParseException(Option(cmd), message, start, stop)
  }
}