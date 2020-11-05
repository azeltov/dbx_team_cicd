# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

streamingCustomers = spark.read.parquet("/mnt/rawsqlcloudstreaming/dbo.Customers.parquet")


# COMMAND ----------

streamingAddresses = spark.read.parquet("/mnt/rawsqlcloudstreaming/dbo.Addresses.parquet")
streamingAddresses = streamingAddresses.withColumnRenamed("CustomerID","SourceSystemCustomerID")
streamingAddresses.registerTempTable('streamingAddresses')

# COMMAND ----------

streamingCustomers =streamingCustomers.withColumn("SourceSystemId",southridgeSource)
streamingCustomers = streamingCustomers.withColumn("GlobalCustomerID",uuidUdf())
streamingCustomers = streamingCustomers.withColumnRenamed("CustomerID","SourceSystemCustomerID")
streamingCustomers.registerTempTable('streamingCustomers')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS streamingCustomersWithAddr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE streamingCustomersWithAddr AS 
# MAGIC SELECT SourceSystemId,
# MAGIC GlobalCustomerID,
# MAGIC streamingCustomers.SourceSystemCustomerID,
# MAGIC LastName,
# MAGIC FirstName,
# MAGIC AddressLine1,
# MAGIC AddressLine2,
# MAGIC City,
# MAGIC State,
# MAGIC ZipCode,
# MAGIC PhoneNumber,
# MAGIC streamingCustomers.CreatedDate as CreatedDate,
# MAGIC streamingCustomers.UpdatedDate as UpdatedDate
# MAGIC FROM streamingCustomers
# MAGIC LEFT JOIN streamingAddresses
# MAGIC ON streamingCustomers.SourceSystemCustomerID = streamingAddresses.SourceSystemCustomerID;

# COMMAND ----------

#streamingCustomers.write.parquet("/mnt/silver/customers",mode='append')

# COMMAND ----------

salesCustomers = spark.read.parquet("/mnt/rawsqlcloudsales/dboCustomers.parquet")
salesCustomers =salesCustomers.withColumn("SourceSystemId",southridgeSource)
salesCustomers = salesCustomers.withColumn("GlobalCustomerID",uuidUdf())
salesCustomers = salesCustomers.withColumnRenamed("CustomerID","SourceSystemCustomerID")
salesCustomers.registerTempTable('salesCustomers')

# COMMAND ----------

sqlAddresses = spark.read.parquet("/mnt/rawsqlcloudsales/dboAddresses.parquet")
sqlAddresses = sqlAddresses.withColumnRenamed("CustomerID","SourceSystemCustomerID")
sqlAddresses.registerTempTable('sqlAddresses')

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS sqlCustomersWithAddr

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sqlCustomersWithAddr AS 
# MAGIC SELECT SourceSystemId,
# MAGIC GlobalCustomerID,
# MAGIC salesCustomers.SourceSystemCustomerID,
# MAGIC LastName,
# MAGIC FirstName,
# MAGIC AddressLine1,
# MAGIC AddressLine2,
# MAGIC City,
# MAGIC State,
# MAGIC ZipCode,
# MAGIC PhoneNumber,
# MAGIC salesCustomers.CreatedDate as CreatedDate,
# MAGIC salesCustomers.UpdatedDate as UpdatedDate
# MAGIC FROM salesCustomers
# MAGIC LEFT JOIN sqlAddresses
# MAGIC ON salesCustomers.SourceSystemCustomerID = sqlAddresses.SourceSystemCustomerID;

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
fourthcoffeeCustomers.registerTempTable('fourthcoffeeCustomers')

# COMMAND ----------

table('streamingCustomersWithAddr')

# COMMAND ----------

table('sqlCustomersWithAddr')

# COMMAND ----------

table('fourthcoffeeCustomers')

# COMMAND ----------

sqlservervanarsdelCustomers = spark.read.parquet("/mnt/rawsqlservervanarsdel/dboCustomers.parquet")
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumn("SourceSystemId",vanArsdelLtdSource)
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumn("GlobalCustomerID",uuidUdf())
sqlservervanarsdelCustomers = sqlservervanarsdelCustomers.withColumnRenamed("CustomerID","SourceSystemCustomerID")
sqlservervanarsdelCustomers.registerTempTable('sqlservervanarsdelCustomers')
display(sqlservervanarsdelCustomers)

# COMMAND ----------

# MAGIC 
# MAGIC %sql DROP TABLE IF EXISTS masterCustomer

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE masterCustomer AS 
# MAGIC select * from streamingCustomersWithAddr
# MAGIC UNION
# MAGIC select * from sqlCustomersWithAddr
# MAGIC UNION
# MAGIC select * from fourthcoffeeCustomers
# MAGIC UNION
# MAGIC select * from sqlservervanarsdelCustomers

# COMMAND ----------

# MAGIC %md 
# MAGIC select * from masterCustomer

# COMMAND ----------

masterCustomer = table("masterCustomer")

masterCustomer.write \
  .format("com.databricks.spark.sqldw") \
  .option("url", url) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("dbTable", "dbo.DimCustomers") \
  .option("tempDir", "wasbs://temp@ohteam2datalake.blob.core.windows.net/dwtmp") \
  .save()

# COMMAND ----------

# MAGIC %sql SELECT count(*) from masterCustomer

# COMMAND ----------

