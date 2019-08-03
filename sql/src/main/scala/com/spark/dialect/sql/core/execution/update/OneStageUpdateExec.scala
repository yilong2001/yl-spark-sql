package com.spark.dialect.sql.core.execution.update

import com.spark.dialect.sql.core.catalyst.rules.{ExpressionTransformHelper, ReplaceAttributeExp}
import com.spark.dialect.sql.util.{ExpressionEvalHelper, StrUtils}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BindReferences, BoundReference, EqualTo, Expression, JoinedRow, Or, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable

/**
  * Created by yilong on 2019/6/8.
  */


case class OneStageUpdateExec(input: Seq[Expression],
                              output: Seq[Attribute],
                              target: TableIdentifier,
                              sets: Map[Attribute,Expression],
                              buildPlanKeys: Seq[Expression],
                              indexPlanKeys: Seq[Expression],
                              condition: Option[Expression],
                              buildSparkPlan: SparkPlan,
                              indexSparkPlan: SparkPlan,
                              indexPlan: LogicalPlan)
  extends org.apache.spark.sql.execution.UnaryExecNode
  /* with CodegenSupport */ {
  final val logger = Logger.getLogger("UpdateOperationWholeExec")

  override def child: SparkPlan = buildSparkPlan

  override def outputPartitioning: Partitioning = child.outputPartitioning

  val defaultPartitionNum = 2

  //TODO:
  override def requiredChildDistribution: Seq[Distribution] = {
    HashClusteredDistribution(buildPlanKeys, Some(defaultPartitionNum)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    logger.info(s"************ doExecute:: input : ${input}; output : ${output}; child: ${child} ")

    doIteratorBuildExec()
    //doBatchIndexPlanExec()
    //doForeachIndexPlanExec()
  }

  def doBatchIndexPlanExec() : RDD[InternalRow] = {
    val sparkSession = sqlContext.sparkSession

    val buildPlanOutputSchema: StructType = StructType(buildSparkPlan.output.map(a => new StructField(a.name, a.dataType)))

    val allAttr = mutable.ArrayBuffer[Attribute]()
    val structFieldArr = new mutable.ArrayBuffer[StructField]()
    (indexSparkPlan.output ++ buildSparkPlan.output).foreach(x => {
      allAttr.append(x)
      structFieldArr.append(new StructField(x.name, x.dataType))
    })
    val allAttrWithNull = allAttr.map(_.withNullability(true))

    val buildBindReferences = new mutable.HashMap[String, BoundReference]()
    buildSparkPlan.output.foreach(x => {
      val bind = BindReferences.bindReference[Expression](x, allAttrWithNull)
      buildBindReferences.put(x.name, bind.asInstanceOf[BoundReference])
    }
    )

    val childOutputSchema: StructType = StructType(structFieldArr)

    val buildKeys = new mutable.ArrayBuffer[Expression]()
    buildPlanKeys.foreach(x => buildKeys.append(x))

    val buildRDD = buildSparkPlan.executeCollect()
    val indexedPlans = new mutable.ArrayBuffer[SparkPlan]()

    buildRDD.foreach {
      row => {
        val valueMap = new mutable.HashMap[String, Any]()

        buildPlanOutputSchema.foreach(sf => {
          val dt = buildBindReferences.get(sf.name).get
          valueMap.put(sf.name, row.get(dt.ordinal, dt.dataType));
        })

        val queryExec = new QueryExecution(sparkSession,
          UpdateConstantReplacer.updateIndexPlanCondition(buildKeys.toSeq, buildPlanOutputSchema, valueMap.toMap, indexPlan)).executedPlan
        indexedPlans.append(queryExec)
      }
    }

    val allBindAttributes = allAttrWithNull.map(x =>
      BindReferences.bindReference[Expression](x, allAttrWithNull)
    )

    val step1 = indexedPlans.zip(buildRDD).map(tp => {
      tp._1.execute().mapPartitions(rowIt => rowIt.map(row => {
        val newrow = new JoinedRow(row, tp._2)
        val unsafeProjection = UnsafeProjection.create(allBindAttributes)
        unsafeProjection(newrow)
      }))
    }).reduce(_.union(_))

    val mySets = new mutable.HashMap[Attribute, Expression]()
    sets.foreach(tp => mySets.put(tp._1, tp._2))

    val destTable = target.copy()
    step1.mapPartitions(row => {
      UpdatePerformerIterator(sparkSession, destTable, row, childOutputSchema, allAttr, mySets.toMap)
    })
  }

  def doForeachIndexPlanExec() : RDD[InternalRow] = {
    val sparkSession = sqlContext.sparkSession

    val buildPlanOutputSchema: StructType = StructType(buildSparkPlan.output.map(a => new StructField(a.name, a.dataType)))

    val allAttr = mutable.ArrayBuffer[Attribute]()
    val structFieldArr = new mutable.ArrayBuffer[StructField]()
    (indexSparkPlan.output ++ buildSparkPlan.output).foreach(x => {
      allAttr.append(x)
      structFieldArr.append(new StructField(x.name, x.dataType))
    })
    val allAttrWithNull = allAttr.map(_.withNullability(true))

    val buildBindReferences = new mutable.HashMap[String, BoundReference]()
    buildSparkPlan.output.foreach(x => {
      val bind = BindReferences.bindReference[Expression](x, allAttrWithNull)
      buildBindReferences.put(x.name, bind.asInstanceOf[BoundReference])
    }
    )

    val buildRDD = buildSparkPlan.executeCollect()
    val indexedPlans = new mutable.ArrayBuffer[SparkPlan]()

    val allBindAttributes = allAttrWithNull.map(x =>
      BindReferences.bindReference[Expression](x, allAttrWithNull)
    )

    val childOutputSchema: StructType = StructType(structFieldArr)
    val mySets = new mutable.HashMap[Attribute, Expression]()
    sets.foreach(tp => mySets.put(tp._1, tp._2))

    val buildKeys = new mutable.ArrayBuffer[Expression]()
    buildPlanKeys.foreach(x => buildKeys.append(x))

    val destTable = target.copy()

    buildRDD.map {
      lrow => {
        val valueMap = new mutable.HashMap[String, Any]()

        buildPlanOutputSchema.foreach(sf => {
          val dt = buildBindReferences.get(sf.name).get
          valueMap.put(sf.name, lrow.get(dt.ordinal, dt.dataType));
        })

        val queryExec = new QueryExecution(sparkSession,
          UpdateConstantReplacer.updateIndexPlanCondition(buildKeys.toSeq, buildPlanOutputSchema, valueMap.toMap, indexPlan)).executedPlan
        queryExec.execute().mapPartitions(rrowIt => {
          val nrrowIt = rrowIt.map(rrow => {
            val newrow = new JoinedRow(rrow, lrow)
            val unsafeProjection = UnsafeProjection.create(allBindAttributes)
            unsafeProjection(newrow)
          })

          UpdatePerformerIterator(sparkSession, destTable, nrrowIt, childOutputSchema, allAttr, mySets.toMap)
        })
      }
    }.reduce(_.union(_))
  }

  def doIteratorBuildExec() : RDD[InternalRow] = {
    val sparkSession = sqlContext.sparkSession

    val buildPlanOutputSchema: StructType = StructType(buildSparkPlan.output.map(a => new StructField(a.name, a.dataType)))

    val allAttr = mutable.ArrayBuffer[Attribute]()
    val structFieldArr = new mutable.ArrayBuffer[StructField]()
    (indexSparkPlan.output ++ buildSparkPlan.output).foreach(x => {
      allAttr.append(x)
      structFieldArr.append(new StructField(x.name, x.dataType))
    })
    val allAttrWithNull = allAttr.map(_.withNullability(true))

    val buildBindReferences = new mutable.HashMap[String, BoundReference]()
    buildSparkPlan.output.foreach(x => {
      val bind = BindReferences.bindReference[Expression](x, allAttrWithNull)
      buildBindReferences.put(x.name, bind.asInstanceOf[BoundReference])
    }
    )

    val buildRDD = buildSparkPlan.executeToIterator()
    val indexedPlans = new mutable.ArrayBuffer[SparkPlan]()

    val allBindAttributes = allAttrWithNull.map(x =>
      BindReferences.bindReference[Expression](x, allAttrWithNull)
    )

    val childOutputSchema: StructType = StructType(structFieldArr)
    val mySets = new mutable.HashMap[Attribute, Expression]()
    sets.foreach(tp => mySets.put(tp._1, tp._2))

    val buildKeys = new mutable.ArrayBuffer[Expression]()
    buildPlanKeys.foreach(x => buildKeys.append(x))

    val destTable = target.copy()

    buildRDD.map {
      lrow => {
        val valueMap = new mutable.HashMap[String, Any]()

        buildPlanOutputSchema.foreach(sf => {
          val dt = buildBindReferences.get(sf.name).get
          valueMap.put(sf.name, lrow.get(dt.ordinal, dt.dataType));
        })

        val queryExec = new QueryExecution(sparkSession,
          UpdateConstantReplacer.updateIndexPlanCondition(buildKeys.toSeq, buildPlanOutputSchema, valueMap.toMap, indexPlan)).executedPlan
        queryExec.execute().mapPartitions(rrowIt => {
          val nrrowIt = rrowIt.map(rrow => {
            val newrow = new JoinedRow(rrow, lrow)
            val unsafeProjection = UnsafeProjection.create(allBindAttributes)
            unsafeProjection(newrow)
          })

          UpdatePerformerIterator(sparkSession, destTable, nrrowIt, childOutputSchema, allAttr, mySets.toMap)
        })
      }
    }.reduce(_.union(_))
  }
}
