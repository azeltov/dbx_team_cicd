# Databricks notebook source

spark.conf.set(
  "fs.azure.account.key.ohteam2datalake.blob.core.windows.net",
  dbutils.secrets.get(scope = "team2", key = "blobkey"))
#encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;

# COMMAND ----------

url="jdbc:sqlserver://ohteam2synapseworkspace.sql.azuresynapse.net:1433;database=OHTeam2_Silver;user=southridge@ohteam2synapseworkspace;password={};".format(dbutils.secrets.get(scope = "team2", key = "synapsepasswd"))
url

# COMMAND ----------

from pyspark.sql.functions import *
southridgeSource = lit("1")
fourthCoffeeSource= lit("2")
vanArsdelLtdSource= lit("3")

# COMMAND ----------

import uuid
from pyspark.sql.types import *
from pyspark.sql.functions import udf
uuidUdf= udf(lambda : str(uuid.uuid4()),StringType())

# COMMAND ----------

