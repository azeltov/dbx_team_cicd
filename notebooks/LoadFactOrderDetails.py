# Databricks notebook source
# MAGIC %md
# MAGIC #Populate FactOrderDetails

# COMMAND ----------

# MAGIC %run ./Utils

# COMMAND ----------

import uuid
from pyspark.sql.functions import udf
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC CREATE TABLE dbo.FactOrderDetails
# MAGIC (
# MAGIC 	[SourceSystemID] int NOT NULL,
# MAGIC 	[GlobalOrderDetailsID] nvarchar(256) NULL,
# MAGIC 	[SourceSystemOrderID] nvarchar(256) NULL,	
# MAGIC 	[SourceSystemOrderDetailsID] nvarchar(256) NULL,
# MAGIC 	[MovieID] nvarchar(256) NULL,
# MAGIC 	[Quantity] int NULL,
# MAGIC 	[UnitCost] decimal NULL,
# MAGIC 	[LineNumber] int NULL,
# MAGIC 	[OrderDetailsCreateDate] date NULL		
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC 
# MAGIC streamingOrderDetails = spark.read.parquet("/mnt/rawsqlcloudsales/dboOrderDetails.parquet")
# MAGIC display(streamingOrderDetails)
# MAGIC 
# MAGIC 
# MAGIC ```

# COMMAND ----------

# Define FactOrderDetails schema
from pyspark.sql.types import *
FOrderDetailsSchema = StructType ([StructField("SourceSystemOrderDetailsID", StringType(), False),
                                StructField("SourceSystemOrderID", StringType(), False),
                                StructField("MovieID", StringType(), False),
                                StructField("Quantity", IntegerType(), False),
                                StructField("UnitCost", DecimalType(), False),
                                StructField("LineNumber", IntegerType(), True),
                                StructField("CreatedDate", DateType(), False),
                                StructField("UpdatedDate", DateType(), False)])

# COMMAND ----------

# Read dboOrderDetails file
factOrderDetails = spark.read.parquet("/mnt/rawsqlcloudsales/dboOrderDetails.parquet", schema=FOrderDetailsSchema)

# COMMAND ----------

from pyspark.sql.functions import *
southridgeSource = lit("1")
fourthCoffeeSource= lit("2")
vanArsdelLtdSource= lit("3")

# Append Sources
factOrderDetails= factOrderDetails.withColumn("SourceSystemId",southridgeSource)

# Apply ID
factOrderDetails = factOrderDetails.withColumn("GlobalOrderID",uuidUdf())

# COMMAND ----------

 display(factOrderDetails)

# COMMAND ----------

# factSales.write.parquet("/mnt/silver/FactOrderDetails")

# COMMAND ----------

factOrderDetails.write \
.format("com.databricks.spark.sqldw") \
.option("url", url) \
.option("forwardSparkAzureStorageCredentials", "true") \
.option("dbTable", "dbo.OrderDetails") \
.option("tempDir", "wasbs://temp@ohteam2datalake.blob.core.windows.net/dwtmp") \
.save()