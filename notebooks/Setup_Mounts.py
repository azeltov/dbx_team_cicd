# Databricks notebook source
# MAGIC %md
# MAGIC ## connection spn to adls gen2

# COMMAND ----------

# authenticate using a service principal and OAuth 2.0
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", "82a887f5-8935-48bd-87e3-2b95d1709497")
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope = "team2", key = "dbxspn"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/9e35c5d0-02d4-48ed-8680-edafa95eaf24/oauth2/token")



# COMMAND ----------


salesCustomers = spark.read.parquet("abfss://rawsqlcloudsales@ohteam2datalake.dfs.core.windows.net/dboCustomers.parquet")
display(salesCustomers)

# COMMAND ----------

streamingCustomers = spark.read.parquet("abfss://rawsqlcloudstreaming@ohteam2datalake.dfs.core.windows.net/dbo.Customers.parquet")


# COMMAND ----------

display(streamingCustomers)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "82a887f5-8935-48bd-87e3-2b95d1709497",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = "team2", key = "dbxspn"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9e35c5d0-02d4-48ed-8680-edafa95eaf24/oauth2/token"}


# COMMAND ----------


dbutils.fs.mount(
  source = "abfss://rawsqlcloudstreaming@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/rawsqlcloudstreaming",
  extra_configs = configs)

# COMMAND ----------


dbutils.fs.mount(
  source = "abfss://rawsqlcloudsales@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/rawsqlcloudsales",
  extra_configs = configs)

# COMMAND ----------

streamingCustomers = spark.read.parquet("/mnt/rawsqlcloudstreaming/dbo.Customers.parquet")
display(streamingCustomers)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://rawcosmosdb@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/rawcosmosdb",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/rawcosmosdb/Movies

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://rawcsvfourthcoffee@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/rawcsvfourthcoffee",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/rawcsvfourthcoffee/CSVFiles/

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://rawsqlservervanarsdel@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/rawsqlservervanarsdel",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/rawsqlservervanarsdel

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://silver@ohteam2datalake.dfs.core.windows.net/",
  mount_point = "/mnt/silver",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls /mnt/silver/

# COMMAND ----------

dbutils.fs.unmount("/mnt/silverdata")

# COMMAND ----------

