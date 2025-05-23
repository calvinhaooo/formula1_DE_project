# Databricks notebook source
# MAGIC %md
# MAGIC Explore DBFS Root
# MAGIC 1. List all the folders in DBFS root
# MAGIC 2. interacti with DBFS File Broswer
# MAGIC 3. Upload file to DBFS Root
# MAGIC
# MAGIC We can use vault in Azure to manage our secret keys. --> it is a kind of service

# COMMAND ----------

display(dbutils.fs.ls("/"))

# COMMAND ----------

display(dbutils.fs.ls("/FileStore"))

# COMMAND ----------

display(spark.read.csv("/FileStore/circuits.csv"))

# COMMAND ----------

