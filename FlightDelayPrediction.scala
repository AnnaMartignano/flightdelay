// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SQLContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types.DoubleType

import scala.math.{abs, log}

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

// COMMAND ----------

val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
val session = cluster.connect()
val sqlContext = new SQLContext(sc)
val dataFrame = sqlContext.read
         .format("org.apache.spark.sql.cassandra")
         .options(Map( "table" -> "flights", "keyspace" -> "flightspace"))
         .load()
dataFrame.show

// COMMAND ----------

val logTrans = (value:Int) => {
   val ret:Double = log(abs(value) + 1)
   if (value != null && value >= 0) ret else -ret
}

val logTransUDF = udf(logTrans)

val square = (value:Int) => value * value
val squareUDF = udf(square)

val toInt = (value:Int) => {
  val ret:Int = value
  if (value != null && value >= 0) ret else -ret
}
val to_Int = udf(toInt)

// CLEANING:
//val dataFrame1 = dataFrame.na.fill("0", Seq("ArrDelay"))
//dataFrame1 = dataFrame.filter(dataFrame("ArrDelay") =!= "NA").filter(dataFrame("DepDelay") =!= "NA")
      //.filter(dataFrame("TaxiOut") =!= "NA").filter(dataFrame("DepTime") =!= "NA")
      //.filter(dataFrame("Month") =!= "NA").filter(dataFrame("Distance") =!= "NA")
//val dataFrame1 = dataFrame.na.replace(dataFrame.CRSElapsedTime,Map("" -> "0.0")).show()

// TRANSFORMING:
val dataFrame1 = dataFrame.withColumn("ArrDelayT", logTransUDF(dataFrame("ArrDelay")))
                 .withColumn("DepDelayT", logTransUDF(dataFrame("DepDelay")))
                 .withColumn("Taxi_Out", to_Int(dataFrame("TaxiOut")))
                 .withColumn("ArrivalDelay", to_Int(dataFrame("ArrDelay")))
                 .withColumn("DepartureDelay", to_Int(dataFrame("DepDelay")))
                 .withColumn("CRSE_lapsedTime", to_Int(dataFrame("CRSElapsedTime")))

val colNames = Seq("DayOfWeek","Month","Distance","CRSE_lapsedTime","Taxi_Out", "DepartureDelay", "ArrivalDelay", "DepDelayT", "ArrDelayT")
val dataFrameFinal = dataFrame1.select(colNames.head, colNames.tail: _ *) // Only colNames


// ADDING FEATURES:
val assembler = new VectorAssembler()
              .setInputCols(Array("DayOfWeek","Month","Distance","CRSE_lapsedTime", "Taxi_Out", "DepartureDelay", "ArrivalDelay", "DepDelayT"))
              .setOutputCol("features")

val dataFrameComplete = assembler.transform(dataFrameFinal)
dataFrameComplete.show

// COMMAND ----------

val Array(train, test) = dataFrameComplete.randomSplit(Array(0.7,0.3))
//train.show
val lm = new LinearRegression().setLabelCol("ArrDelayT")
val model = lm.fit(train)

// COMMAND ----------

val trainingSummary = model.summary
println(s"numIterations: ${trainingSummary.totalIterations}")
println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
trainingSummary.residuals.show()
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
println(s"R2: ${trainingSummary.r2}")

println(s"Coefficient Standard Errors: ${trainingSummary.coefficientStandardErrors.mkString(",")}")
println(s"T Values: ${trainingSummary.tValues.mkString(",")}")
println(s"P Values: ${trainingSummary.pValues.mkString(",")}")

// COMMAND ----------

val holdout = model.transform(test).select("prediction", "ArrDelayT")
val holdoutRes = holdout
                .withColumn("ArrDelayT", holdout("ArrDelayT").cast(DoubleType))
                .withColumn("prediction", holdout("prediction").cast(DoubleType))
val rm = new RegressionMetrics(holdoutRes.rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
println("R2:" + rm.r2)
println("RMSE:" + rm.rootMeanSquaredError)
println("MSE:" + rm.meanSquaredError)
println("Explained Variance:" + rm.explainedVariance)

// COMMAND ----------

holdout.show

// COMMAND ----------

holdout.write.saveAsTable("lmModel")
