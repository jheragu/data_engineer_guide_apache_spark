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

git status

# COMMAND ----------


