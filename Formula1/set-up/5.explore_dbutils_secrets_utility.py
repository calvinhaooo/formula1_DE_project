# Databricks notebook source
# MAGIC %md
# MAGIC Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula12345-scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula12345-scope', key = 'formula12345-account-key')

# COMMAND ----------

# MAGIC %md
# MAGIC you can safely upload this to ur github repo without leaking the key. This is because we set scope in the databrick, only the user with permission can get the key.

# COMMAND ----------

