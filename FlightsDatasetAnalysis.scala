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
