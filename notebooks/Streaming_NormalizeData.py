# Databricks notebook source
streamingCustomers = spark.read.parquet("/mnt/rawsqlcloudstreaming/dbo.Transactions.parquet")
display(streamingCustomers)

# COMMAND ----------

from pyspark.sql.functions import *
southridgeSource = lit("1")
fourthCoffeeSource= lit("2")
vanArsdelLtdSource= lit("3")

# COMMAND ----------

import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

streamingCustomers =streamingCustomers.withColumn("SourceSystemId",southridgeSource)
streamingCustomers = streamingCustomers.withColumn("GlobalTransactionID",uuidUdf())
streamingCustomers = streamingCustomers.withColumnRenamed("CustomerID","CustomerSK")
streamingCustomers = streamingCustomers.withColumnRenamed("MovieID","MovieSK")
streamingCustomers = streamingCustomers.withColumnRenamed("GlobalTransactionID","StreamingSK")


# COMMAND ----------

# MAGIC %python 
# MAGIC import dateutil.parser
# MAGIC import time
# MAGIC import datetime
# MAGIC from dateutil.parser import parse
# MAGIC 
# MAGIC 
# MAGIC datestring = "2017-04-18T20:59:00.000+0000"
# MAGIC datestring2 = "2017-04-18T21:41:00.000+0000"
# MAGIC startdate = dateutil.parser.parse(datestring)
# MAGIC enddate = dateutil.parser.parse(datestring2)
# MAGIC total = enddate - startdate
# MAGIC newtotal = total.total_seconds()/60
# MAGIC print(newtotal)

# COMMAND ----------

streamingCustomers = streamingCustomers.drop("CreatedDate")
streamingCustomers = streamingCustomers.drop("UpdatedDate")


# COMMAND ----------

display(streamingCustomers)

# COMMAND ----------

streamingCustomers.registerTempTable('streamingCustomers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   TransactionID,
# MAGIC   CustomerSK,
# MAGIC   MovieSK,
# MAGIC   months_between(CAST(Substring(StreamEnd,1,10) AS DATE) ,CAST(Substring(StreamStart,1,10) AS DATE))  as DateDiffTest,
# MAGIC   --CASE
# MAGIC     --WHEN (to_timestamp(Substring(StreamEnd,11,18),"HH:mm:ss")) > to_timestamp(Substring(StreamStart,11,18),"HH:mm:ss")))
# MAGIC     --THEN CAST((to_timestamp(Substring(StreamEnd,11,18),"HH:mm:ss")) - to_timestamp(Substring(StreamStart,11,18),"HH:mm:ss")) AS TIMESTAMP)
# MAGIC     
# MAGIC     
# MAGIC     --to_timestamp(Substring(StreamStart,11,18),"HH:mm:ss"))
# MAGIC     
# MAGIC     --ELSE 
# MAGIC     --(to_timestamp(StreamEnd) + (make_timestamp(0,0,1,0,0,0) - to_timestamp(StreamStart)))
# MAGIC     
# MAGIC   --END AS Difference,
# MAGIC   CAST(Substring(StreamStart,1,10) AS DATE) AS StreamStartDateSK,
# MAGIC   Substring(StreamStart,11,19) AS StreamStartTimeSK,
# MAGIC   CAST(Substring(StreamEnd,1,10) AS DATE) AS StreamEndDateSK,
# MAGIC   Substring(StreamEnd,11,19) AS StreamEndTimeSK,
# MAGIC   SourceSystemID,
# MAGIC   StreamingSK
# MAGIC FROM streamingCustomers

# COMMAND ----------

from datetime import datetime,tzinfo,timedelta

streamingCustomers.withColumn("StreamStart",datetime.utcfromtimestamp(col("StreamStart")))
import spark.implicits._

# COMMAND ----------

display(streamingCustomers)

# COMMAND ----------

streamingCustomers.write.parquet("/mnt/silverdata/streaming")

# COMMAND ----------

# Define Fourth Coffee Customer Schema
from pyspark.sql.types import *
FCCustomersSchema = StructType ([StructField("CustomerID", StringType(), False),
                                StructField("LastName", StringType(), False),
                                StructField("FirstName", StringType(), False),
                                StructField("AddressLine1", StringType(), False),
                                StructField("AddressLine2", StringType(), True),
                                StructField("City", StringType(), False),
                                StructField("State", StringType(), False),
                                StructField("ZipCode", StringType(), False),
                                StructField("PhoneNumber", StringType(), False),
                                StructField("CreatedDate", DateType(), False),
                                StructField("UpdatedDate", DateType(), False)])

# COMMAND ----------

# Read Fourth Coffee Customer Data
fourthcoffeeCustomers = spark.read.csv("/mnt/rawcsvfourthcoffee/CSVFiles/Customers.csv", schema=FCCustomersSchema, header="True")

# COMMAND ----------

fourthcoffeeCustomers = fourthcoffeeCustomers.withColumn("SourceSystemId",fourthCoffeeSource)
fourthcoffeeCustomers = fourthcoffeeCustomers.withColumn("GlobalCustomerID",uuidUdf())
fourthcoffeeCustomers = fourthcoffeeCustomers.withColumnRenamed("CustomerID","SourceSystemCustomerID")
display(fourthcoffeeCustomers)

# COMMAND ----------

sqlservervanarsdelCustomers = spark.read.parquet("/mnt/rawsqlservervanarsdel/dboCustomers.parquet")
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumn("SourceSystemId",vanArsdelLtdSource)
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumn("GlobalCustomerID",uuidUdf())
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumnRenamed("CustomerID","SourceSystemCustomerID")
display(sqlservervanarsdelCustomers)

# COMMAND ----------

