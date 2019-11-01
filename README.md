# :rotating_light: WARNING: This is a Data-Intensive Application :rotating_light:

## Team Project for [ID2221 Course](https://id2221kth.github.io/) @KTH

Authors: Anna Martignano and Muhammad Hamza Malik  

This project uses the information made available by the [Bereau of Transportation Statistics](https://www.bts.gov/) to create a linear regression model using [MLlib](https://spark.apache.org/mllib/).  
The methodology used to process this data consists in:  
1. Data cleaning opeations performed in R to remove not alphanumeric characters, null values, etc.
2. Upload rows into the column-oriented NoSQL database [Cassandra](http://cassandra.apache.org/)
3. Set up of the connection among [Databricks](https://databricks.com/) and Cassandra to allow the scala notebooks to read the content of the Cassandra keyspace.table
4. Data analysis performed in [SparkSQL](https://spark.apache.org/sql/) and visualization performed with [Tableau](https://www.tableau.com/)
5. Creation of the linear regression model and prediction of the Arrival Delay

If you are interested in a detailed explanation, please have a look at the [report](https://github.com/AnnaMartignano/flightdelay/blob/master/Report.pdf).

