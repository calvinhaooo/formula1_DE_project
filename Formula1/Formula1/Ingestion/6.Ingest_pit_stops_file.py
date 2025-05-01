# Databricks notebook source
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

# MAGIC %run "../includes/common_funtions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pitstop_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstop_df = spark.read\
.schema(pitstop_schema)\
.option("multiline", True)\
.json(f"abfss://raw@formula12345.dfs.core.windows.net/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pitstop_processed_df = pitstop_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(pitstop_processed_df)

# COMMAND ----------

# pitstop_processed_df.write.mode("append").parquet("abfss://processed@formula12345.dfs.core.windows.net/pit_stops")
# pitstop_processed_df.write.mode("append").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.stop = src.stop AND tgt.driver_id = src.driver_id"
merge_delta_data(pitstop_processed_df, 'f1_processed', 'pit_stops', processed_file_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

