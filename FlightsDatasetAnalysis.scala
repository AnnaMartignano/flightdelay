// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.types._

import scala.math.abs
import scala.math.log

import org.apache.spark.sql.SQLContext

// COMMAND ----------

val schema = new StructType()
  .add("Year", IntegerType)
  .add("Month", IntegerType)
  .add("DayofMonth", IntegerType)
  .add("DayOfWeek", IntegerType)
  .add("DepTime", IntegerType)
  .add("CRSDepTime", IntegerType)
  .add("ArrTime", IntegerType)
  .add("CRSArrTime", IntegerType)
  .add("UniqueCarrier", StringType)
  .add("FlightNum", IntegerType)
  .add("TailNum", StringType)
  .add("ActualElapsedTime", IntegerType)
  .add("CRSElapsedTime", IntegerType)
  .add("AirTime", IntegerType)
  .add("ArrDelay", IntegerType)
  .add("DepDelay", IntegerType)
  .add("Origin", StringType)
  .add("Dest", StringType)
  .add("Distance", IntegerType)
  .add("TaxiIn", IntegerType)
  .add("TaxiOut", IntegerType)
  .add("Cancelled", IntegerType)
  .add("CancellationCode", StringType)
  .add("Diverted", IntegerType)
  .add("CarrierDelay", IntegerType)
  .add("WeatherDelay", IntegerType)
  .add("Sdelay", IntegerType)
  .add("SecurityDelay", IntegerType)
  .add("LateAircraftDelay", IntegerType)

// COMMAND ----------

// Loading Dataset:
val sparkSes = SparkSession.builder().appName("Flight delay prediction").getOrCreate()
val sqlContext = new SQLContext(sc)
val csvfile = "/FileStore/tables/flights.csv"

//Enforced Schema
val dataFrame = sqlContext.read.option("inferSchema","false").option("header","true").schema(schema).csv(csvfile)

dataFrame.show
dataFrame.count()

// COMMAND ----------

dataFrame.describe("Distance","CRSElapsedTime","TaxiOut","DepDelay","ArrDelay").show
dataFrame.count()

// COMMAND ----------

val df = dataFrame.withColumn("delayed", expr("ArrDelay > 0")) 
//create a table for Tableau
df.write.saveAsTable("flightsoriginal")
//create a view for Spark SQL
df.createOrReplaceTempView("flight_view")

// COMMAND ----------

//Top 10 Longest departure delays
val londdelayDF = spark.sql("select UniqueCarrier, DepDelay from flight_view order by DepDelay desc limit 10").show()

// COMMAND ----------

//Average Departure Delay by Carrier
val avgpercarrierDF = spark.sql("select UniqueCarrier, avg(DepDelay), avg(ArrDelay) from flight_view group by UniqueCarrier order by avg(DepDelay) desc").show()

// COMMAND ----------

//Count of Departure Delays by Carrier (where delay >= 1 hour)
val countpercarrierDF = spark.sql("select UniqueCarrier, count(DepDelay), count(ArrDelay) from flight_view where DepDelay >= 60 and ArrDelay >=60 group by UniqueCarrier order by count(DepDelay) desc").show()

// COMMAND ----------

//Count of Departure Delays by Day of Week
val delayedperdayDF = spark.sql("select DayOfWeek, count(DepDelay) from flight_view where delayed='true' group by DayOfWeek order by DayOfWeek").show()

// COMMAND ----------

//Count of Departure Delays by Origin
val delayedbyoriginDF = spark.sql("select Origin, count(DepDelay) from flight_view where delayed='true' group by Origin order by count(DepDelay) desc").show()

// COMMAND ----------

//Count of Departure Delays by Destination
val delayedbydestDF = spark.sql("select Dest, count(DepDelay) from flight_view where delayed='true' group by Dest order by count(DepDelay) desc").show()

// COMMAND ----------

//Count of Departure Delays by Origin and Destination
val delayedbyorigindestDF = spark.sql("select Origin, Dest, count(DepDelay) from flight_view where delayed='true' group by Origin, Dest order by count(DepDelay) desc").show()

// COMMAND ----------

//Stats per origin Airport: delayed flights over the total amount
val delayedbyoriginDF = spark.sql("select Origin, sum(case when delayed = 'true' then 1 else 0 end)/COUNT(*) as pct_delayed from flight_view group by Origin order by pct_delayed desc").show()
