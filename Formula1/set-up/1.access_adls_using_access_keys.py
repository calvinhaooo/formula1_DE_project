# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read the data from circuits.csv file
# MAGIC
# MAGIC However, using this method, you cannot set specific permissions to users, such only-read, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC Below, I use a variable to store the secret. 

# COMMAND ----------

fomula1dl_account_key = dbutils.secrets.get(scope = 'formula12345-scope', key = 'formula12345-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula12345.dfs.core.windows.net",
    fomula1dl_account_key
)

# COMMAND ----------

dbutils.fs.ls("abfss://demo@formula12345.dfs.core.windows.net")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula12345.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

