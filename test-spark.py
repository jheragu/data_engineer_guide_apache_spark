# Databricks notebook source
# MAGIC %md
# MAGIC ##The SparkSession

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrames

# COMMAND ----------

# MAGIC %md
# MAGIC ####Partitions

# COMMAND ----------

myRange = spark.range(1000).toDF("number")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformations

# COMMAND ----------

divisBy2 = myRange.where("number % 2 = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Actions

# COMMAND ----------

divisBy2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####End to End Example

# COMMAND ----------

flightData2015 = spark.read.option("inferschema", "true").option("header", "true").csv("/FileStore/shared_uploads/jesus.hernandez@tzm.de/flight-data/2015_summary.csv")

# COMMAND ----------

flightData2015.take(3)

# COMMAND ----------

flightData2015.sort("count").explain()

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "5")
flightData2015.sort("count").take(2)


# COMMAND ----------

# MAGIC %md
# MAGIC ##DataFrames and SQL 

# COMMAND ----------

# MAGIC %md
# MAGIC #### First Example

# COMMAND ----------

#make DataFrame into a table or view
flightData2015.createOrReplaceTempView("flight_data_2015")

# COMMAND ----------

sqlWay = spark.sql(""" SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME """)

dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

sqlWay.explain()

dataFrameWay.explain()

# COMMAND ----------

#maximum number of flights to and from any given location are
spark.sql("SELECT max(count) FROM flight_data_2015").take(1)

# COMMAND ----------

#maximum number of flights to and from any given location are
from pyspark.sql.functions import max
flightData2015.select(max("count")).take(1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Second Example (Multitransformation)

# COMMAND ----------

maxSql = spark.sql(""" SELECT DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC LIMIT 5 """)

maxSql.collect()

# COMMAND ----------

#DataFrame Syntax
from pyspark.sql.functions import desc

flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##APIs

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Structured Streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Using DataFrames

# COMMAND ----------

staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("FileStore/shared_uploads/jesus.hernandez@tzm.de/retail_data/by-day/*.csv")

# COMMAND ----------

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

# COMMAND ----------

from pyspark.sql.functions import window, column, desc, col 
staticDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 Day")).sum("total_cost").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Using Streams (only for Demo, don´t use in production)

# COMMAND ----------

# maxFilesTrigger to 1 is only used to simulate the stream: number of files we should read in at once. Ommit on production
streamingDataFrame = spark.readStream.schema(staticSchema).option("maxFilesTrigger", 1).format("csv").option("header", "true").load("FileStore/shared_uploads/jesus.hernandez@tzm.de/retail_data/by-day/*.csv")

# COMMAND ----------

streamingDataFrame.isStreaming

# COMMAND ----------

# could be queried with pure SQL
purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 Day")).sum("total_cost")

# COMMAND ----------

# Streaming Action. In Memory
purchaseByCustomerPerHour.writeStream.format("memory").queryName("customer_purchases").outputMode("complete").start()

# COMMAND ----------

# SQL Query against the stream to debug
spark.sql( """ SELECT * FROM customer_purchases ORDER BY 'sum(total_cost)' DESC """).show(5)

# COMMAND ----------

# Streaming Action. In Console
purchaseByCustomerPerHour.writeStream.format("console").queryName("customer_purchases_2").outputMode("complete").start()


# COMMAND ----------


