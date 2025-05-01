# Databricks notebook source
# MAGIC %md
# MAGIC Access Azure Data Lake using credentials passthrough
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read the data from circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC spark.conf.set(
# MAGIC     "fs.azure.account.key.formula12345.dfs.core.windows.net",
# MAGIC     "Mz6FQpm/VRxVp/NuL+NLwlJ2TE+qQiZ2StnesVR9KOiY5i6d2INb87Rj5tVT6G+vUHj1kMwa+hdZ+AStGET8DQ== "
# MAGIC ) --> before, we need this command to authenticate our access key.
# MAGIC
# MAGIC Then, if we set cluster scoped credentials in the cluster, we do not have to authenticate the key for the notebooks. The notebook is in the cluster. 
# MAGIC To be clear, the ket is stored in within the cluster and the cluster is done all the authentication for users.

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula12345.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula12345.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

