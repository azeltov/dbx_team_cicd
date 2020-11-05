# Databricks notebook source
# MAGIC %md
# MAGIC #Populate FactSales

# COMMAND ----------

# MAGIC %run ./Utils

# COMMAND ----------

import uuid
from pyspark.sql.functions import udf
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

streamingOrders = spark.read.parquet("/mnt/rawsqlcloudsales/dboOrders.parquet")
display(streamingOrders)

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC CREATE TABLE [dbo].[FactSales]
# MAGIC (
# MAGIC 	[SourceSystemID] int NOT NULL,
# MAGIC 	[GlobalOrderID] nvarchar(256) NULL,
# MAGIC 	[SourceSystemOrderID] nvarchar(256) NULL,
# MAGIC 	[CustomerID] nvarchar(256) NULL,
# MAGIC 	[OrderDate] date NULL,
# MAGIC 	[ShipDate] date NULL,
# MAGIC 	[TotalCost] decimal NULL,
# MAGIC 	[OrderCreatedDate] date NULL,
# MAGIC 	[OrderUpdatedDate] date
# MAGIC 
# MAGIC )
# MAGIC ```

# COMMAND ----------

# Define FactSales schema
from pyspark.sql.types import *
FSalesSchema = StructType ([StructField("SourceSystemOrderID", StringType(), False),
                            StructField("CustomerID", StringType(), False),
                            StructField("OrderDate", DateType(), False),
                            StructField("ShipDate", DateType(), False),
                            StructField("TotalCost", DecimalType(), True),
                            StructField("CreatedDate", DateType(), False),
                            StructField("UpdatedDate", DateType(), False)])

# COMMAND ----------

# Read dboOrder file
factSales = spark.read.parquet("/mnt/rawsqlcloudsales/dboOrders.parquet", schema=FSalesSchema)

# COMMAND ----------

from pyspark.sql.functions import *
southridgeSource = lit("1")
fourthCoffeeSource= lit("2")
vanArsdelLtdSource= lit("3")

# Append Sources
factSales= factSales.withColumn("SourceSystemId",southridgeSource)

# Apply ID
factSales = factSales.withColumn("GlobalOrderID",uuidUdf())

# COMMAND ----------

# display(factSales)

# COMMAND ----------

# factSales.write.parquet("/mnt/silver/FactSales")

# COMMAND ----------

factSales.write \
.format("com.databricks.spark.sqldw") \
.option("url", url) \
.option("forwardSparkAzureStorageCredentials", "true") \
.option("dbTable", "dbo.FactSales") \
.option("tempDir", "wasbs://temp@ohteam2datalake.blob.core.windows.net/dwtmp") \
.save()