# Databricks notebook source
# MAGIC %md
# MAGIC ### Fourth Coffee Actors

# COMMAND ----------

#%fs ls /mnt/rawcsvfourthcoffee/CSVFiles

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC FactRentals
# MAGIC Column Name	Data Type	Nulls	Rules
# MAGIC RentalSK	32-bit Integer	No	Sequential integer key
# MAGIC TransactionID	GUID	No	Transaction ID from source system
# MAGIC CustomerSK	32-bit Integer	No	Surrogate key from the Customers dimension for the customer
# MAGIC LocationSK	16-bit Integer	No	Surrogate key from the Locations dimension for the location of the source data
# MAGIC MovieSK	32-bit Integer	No	Surrogate key from the Movies dimension for the movie
# MAGIC RentalDateSK	32-bit Integer	No	Surrogate key from the Date dimension for the date the movie was rented
# MAGIC ReturnDateSK	32-bit Integer	Yes	Surrogate key from the Date dimension for the date the movie was returned
# MAGIC RentalDuration	8-bit Integer	Yes	Number of days from when a movie was rented until it was returned
# MAGIC RentalCost	Currency	No	Cost for the base rental
# MAGIC LateFee	Currency	Yes	Late charges for the rental
# MAGIC TotalCost	Currency	Yes	RentalCost + LateFee
# MAGIC RewindFlag	Boolean	Yes	Was the movie rewound?
# MAGIC Data Generation/Population Rules
# MAGIC This is a fact table representing physical movie rentals
# MAGIC Data should be the combination of customer records from all source systems that deal with movie rentals
# MAGIC ```

# COMMAND ----------

# Define Fourth Coffee Rentals Schema
# https://spark.apache.org/docs/latest/sql-ref-datatypes.html
from pyspark.sql.types import *
FCRentalsSchema = StructType ([StructField("TransactionID", StringType(), False),
                                StructField("CustomerID", StringType(), False),
                                StructField("MovieID", StringType(), False),
                                StructField("RentalDate", StringType(), False),
                                StructField("ReturnDate", StringType(), True),
                                StructField("RentalCost", DecimalType(19,4), False),
                                StructField("LateFee", DecimalType(19,4), False),
                                StructField("RewindFlag", ByteType(), False),
                                StructField("CreatedDate", DateType(), False),
                                StructField("UpdatedDate", DateType(), False)])

# COMMAND ----------

FC_Rentals = spark.read.csv("/mnt/rawcsvfourthcoffee/CSVFiles/Transactions.csv", inferSchema=True, header=True)
#display(FC_Rentals)

# COMMAND ----------

FC_Rentals = spark.read.csv("/mnt/rawcsvfourthcoffee/CSVFiles/Transactions.csv", schema=FCRentalsSchema, header=True)
#display(FC_Rentals)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Van Arsdel Rentals

# COMMAND ----------

#%fs ls /mnt/rawsqlservervanarsdel/

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC CREATE TABLE [dbo].[FactRentals] (
# MAGIC     [RentalSK]       INT              NOT NULL,
# MAGIC     [TransactionID]  UNIQUEIDENTIFIER NOT NULL,
# MAGIC     [CustomerSK]     INT              NOT NULL,
# MAGIC     [LocationSK]     SMALLINT         NOT NULL,
# MAGIC     [MovieSK]        INT              NOT NULL,
# MAGIC     [RentalDateSK]   INT              NOT NULL,
# MAGIC     [ReturnDateSK]   INT              NULL,
# MAGIC     [RentalDuration] TINYINT          NULL,
# MAGIC     [RentalCost]     MONEY            NOT NULL,
# MAGIC     [LateFee]        MONEY            NULL,
# MAGIC     [TotalCost]      MONEY            NULL,
# MAGIC     [RewindFlag]     BIT              NULL
# MAGIC )
# MAGIC WITH (CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH([CustomerSK]));
# MAGIC ```

# COMMAND ----------

VA_Rentals = spark.read.parquet("/mnt/rawsqlservervanarsdel/dboTransactions.parquet")
#display(VA_Rentals)

# COMMAND ----------

from pyspark.sql.functions import *
southridgeSource = lit("1")
fourthCoffeeSource = lit("2")
vanArsdelLtdSource = lit("3")

# Append Sources
VA_Rentals = VA_Rentals.withColumn("SourceSystemId",vanArsdelLtdSource)
FC_Rentals = FC_Rentals.withColumn("SourceSystemId",fourthCoffeeSource)
VA_Rentals.registerTempTable('VA_Rentals')
FC_Rentals.registerTempTable('FC_Rentals')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Fourth Coffee and Van Arsdel
# MAGIC 
# MAGIC Date functions https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html#date-and-timestamp-functions

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC %sql
# MAGIC SELECT
# MAGIC   UPPER(TransactionID) AS SourceSystemTransactionID,
# MAGIC   UPPER(CustomerID) AS CustomerID,
# MAGIC   UPPER(MovieID) AS MovieID,
# MAGIC   to_date(RentalDate, 'yyyyMMdd') RentalDate,
# MAGIC   to_date(ReturnDate, 'yyyyMMdd') ReturnDate,
# MAGIC   CASE
# MAGIC     WHEN ReturnDate is not null THEN datediff(to_date(ReturnDate, 'yyyyMMdd'),to_date(RentalDate, 'yyyyMMdd'))
# MAGIC     ELSE null
# MAGIC   END AS RentalDuration,  
# MAGIC   RentalCost,
# MAGIC   LateFee,
# MAGIC   CASE
# MAGIC     WHEN RewindFlag = 1 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS RewindFlag,
# MAGIC   CreatedDate,
# MAGIC   UpdatedDate,
# MAGIC   SourceSystemID
# MAGIC FROM FC_Rentals
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC %sql
# MAGIC SELECT
# MAGIC   UPPER(TransactionID) AS SourceSystemTransactionID,
# MAGIC   UPPER(CustomerID) AS CustomerID,
# MAGIC   UPPER(MovieID) AS MovieID,
# MAGIC   to_date(CAST(RentalDate AS String), 'yyyyMMdd') RentalDate,
# MAGIC   to_date(CAST(ReturnDate AS String), 'yyyyMMdd') ReturnDate,
# MAGIC   CASE
# MAGIC     WHEN ReturnDate is not null THEN datediff(to_date(CAST(ReturnDate AS String), 'yyyyMMdd'),to_date(CAST(RentalDate AS String), 'yyyyMMdd'))
# MAGIC     ELSE null
# MAGIC   END AS RentalDuration,    
# MAGIC   RentalCost,
# MAGIC   LateFee,
# MAGIC   RewindFlag,
# MAGIC   CreatedDate,
# MAGIC   UpdatedDate,
# MAGIC   SourceSystemID
# MAGIC FROM VA_Rentals
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Rentals

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Rentals AS
# MAGIC SELECT
# MAGIC   UPPER(TransactionID) AS SourceSystemTransactionID,
# MAGIC   UPPER(CustomerID) AS CustomerID,
# MAGIC   UPPER(MovieID) AS MovieID,
# MAGIC   to_date(RentalDate, 'yyyyMMdd') RentalDate,
# MAGIC   to_date(ReturnDate, 'yyyyMMdd') ReturnDate,
# MAGIC   CASE
# MAGIC     WHEN ReturnDate is not null THEN datediff(
# MAGIC       to_date(ReturnDate, 'yyyyMMdd'),
# MAGIC       to_date(RentalDate, 'yyyyMMdd')
# MAGIC     )
# MAGIC     ELSE null
# MAGIC   END AS RentalDuration,
# MAGIC   RentalCost,
# MAGIC   LateFee,
# MAGIC   CASE
# MAGIC     WHEN RewindFlag = 1 THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS RewindFlag,
# MAGIC   CreatedDate,
# MAGIC   UpdatedDate,
# MAGIC   SourceSystemID
# MAGIC FROM
# MAGIC   FC_Rentals
# MAGIC UNION
# MAGIC SELECT
# MAGIC   UPPER(TransactionID) AS SourceSystemTransactionID,
# MAGIC   UPPER(CustomerID) AS CustomerID,
# MAGIC   UPPER(MovieID) AS MovieID,
# MAGIC   to_date(CAST(RentalDate AS String), 'yyyyMMdd') RentalDate,
# MAGIC   to_date(CAST(ReturnDate AS String), 'yyyyMMdd') ReturnDate,
# MAGIC   CASE
# MAGIC     WHEN ReturnDate is not null THEN datediff(
# MAGIC       to_date(CAST(ReturnDate AS String), 'yyyyMMdd'),
# MAGIC       to_date(CAST(RentalDate AS String), 'yyyyMMdd')
# MAGIC     )
# MAGIC     ELSE null
# MAGIC   END AS RentalDuration,
# MAGIC   RentalCost,
# MAGIC   LateFee,
# MAGIC   RewindFlag,
# MAGIC   CreatedDate,
# MAGIC   UpdatedDate,
# MAGIC   SourceSystemID
# MAGIC FROM
# MAGIC   VA_Rentals 

# COMMAND ----------

RentalsDF = table("Rentals")
#display(RentalsDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Global Identifier

# COMMAND ----------

import uuid
from pyspark.sql.functions import udf
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

# Apply ID
RentalsDF = RentalsDF.withColumn("GlobalTransactionID",uuidUdf())
#display(RentalsDF)

# COMMAND ----------

# MAGIC %run ./Utils

# COMMAND ----------

RentalsDF.write \
.format("com.databricks.spark.sqldw") \
.option("url", url) \
.option("forwardSparkAzureStorageCredentials", "true") \
.option("dbTable", "dbo.FactRentals") \
.option("tempDir", "wasbs://temp@ohteam2datalake.blob.core.windows.net/dwtmp") \
.save()

# COMMAND ----------

