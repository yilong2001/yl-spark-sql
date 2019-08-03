package com.spark.dialect.sql.core.execution.update

import com.spark.dialect.sql.core.catalyst.rules.{ExpressionTransformHelper, ReplaceAttributeExp}
import com.spark.dialect.sql.util.{ExpressionEvalHelper, StrUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, GenericInternalRow, JoinedRow, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by yilong on 2019/6/29.
  */
object UpdatePerformerIterator {
  def newSourceFieldName(name:String):String = {
    "src_"+name
  }

  def buildOutSchemaWithSourceField(outputSchema: StructType,updateSet: Map[Attribute, Expression]):StructType = {
    val newsetAttrs = updateSet.keySet.map(att => {
      att.withName(newSourceFieldName(att.name))
    })

    val newoutSchema = StructType(outputSchema.fields ++ newsetAttrs.map(x => StructField(x.name,x.dataType,x.nullable,x.metadata)))

    newoutSchema
  }
}
case class UpdatePerformerIterator(spark: SparkSession,
                                   target: TableIdentifier,
                                   rowIterator: Iterator[InternalRow],
                                   outputSchema: StructType,
                                   outputAttr: Seq[Attribute],
                                   updateSet: Map[Attribute, Expression]) extends Iterator[InternalRow] {
  val _log = Logger.getLogger(s"UpdatePerformerIterator : updateSet : ${updateSet}")

  override def hasNext: Boolean = {
    rowIterator.hasNext
  }

  override def next(): InternalRow = {
    val valueMap = new mutable.HashMap[String,Any]()

    val inNext = rowIterator.next()
    (0 to inNext.numFields-1).foreach(id => {
      _log.info(s" ${outputSchema.apply(id)}, ${inNext.getString(id)}")
    })

    outputSchema.fields.foreach(sf => {
      valueMap.put(sf.name, inNext.get(outputSchema.indexOf(sf), sf.dataType))
    })

    val newoutSchema = UpdatePerformerIterator.buildOutSchemaWithSourceField(outputSchema, updateSet)

    updateSet.foreach(tp => {
      val exp = ReplaceAttributeExp(valueMap.toMap, outputSchema, true)(tp._2)
      val sQLConf = new SQLConf()

      var expVal = ExpressionTransformHelper.resolveExpression(spark, sQLConf)(exp)

      val lastVal = ExpressionEvalHelper.evaluateWithGeneratedMutableProjection(expVal)

      _log.info(s"updatedSet : ${tp}, ${expVal}, ${lastVal}")

      valueMap.put(StrUtils.splitString(tp._1.name, ".").last, lastVal)
    })

    valueMap.foreach(x => _log.info(s"updated value for : ${x}"))

    /***
    val newrow = new SpecificInternalRow(outputSchema)
    outputSchema.foreach(sf => {
      val index = outputSchema.indexOf(sf)
      newrow.update(index, inNext.get(index, sf.dataType))
    })

    valueMap.foreach(x => {
      val sf = outputSchema.apply(x._1)
      val index = outputSchema.indexOf(sf)
      newrow.update(index, valueMap.get(x._1).get)
    })
    ***/

    val newrow = new SpecificInternalRow(newoutSchema)
    outputSchema.foreach(sf => {
      val index = outputSchema.indexOf(sf)
      newrow.update(index, inNext.get(index, sf.dataType))
    })

    valueMap.foreach(x => {
      val sf = outputSchema.apply(x._1)
      val index = outputSchema.indexOf(sf)
      newrow.update(index, valueMap.get(x._1).get)
    })

    updateSet.keySet.foreach(x => {
      val nsf = newoutSchema.apply(UpdatePerformerIterator.newSourceFieldName(x.name))
      val osf = outputSchema.apply(x.name)

      newrow.update(newoutSchema.indexOf(nsf), inNext.get(outputSchema.indexOf(osf), osf.dataType))
    })

    newrow
  }
}
