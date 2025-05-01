# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

fomula1dl_account_key = dbutils.secrets.get(scope = 'formula12345-scope', key = 'formula12345-account-key')

spark.conf.set(
    "fs.azure.account.key.formula12345.dfs.core.windows.net",
    fomula1dl_account_key
)


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

name_schema = StructType(fields = [
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields = [
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
    ])

# COMMAND ----------

drivers_df = spark.read\
.schema(drivers_schema)\
.json(f"abfss://raw@formula12345.dfs.core.windows.net/{v_file_date}/drivers.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col, concat, lit

# COMMAND ----------

drivers_processed_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("driverRef", "driver_ref")\
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(drivers_processed_df)

# COMMAND ----------

driver_final_df = drivers_processed_df.drop(col("url"))

# COMMAND ----------

# driver_final_df.write.mode("append").parquet(f"{processed_file_path}/drivers")
driver_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# display(spark.read.parquet("abfss://processed@formula12345.dfs.core.windows.net/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")