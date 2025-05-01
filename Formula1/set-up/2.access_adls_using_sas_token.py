# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using SAS tokens
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read the data from circuits.csv file

# COMMAND ----------

formula12345_demo_sas_token = dbutils.secrets.get(scope="formula12345-scope", key="formula12345-demo-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula12345.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula12345.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula12345.dfs.core.windows.net", formula12345_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula12345.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula12345.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

