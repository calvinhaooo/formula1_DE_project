# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funtions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option("multiline", True)\
.json(f"abfss://raw@formula12345.dfs.core.windows.net/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("qualifyingId", "qualifying_id")\
    .withColumn("ingestion_date", current_timestamp())\
    .withColumn("data_source", lit(v_data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# qualifying_final_df.write.mode("append").parquet("abfss://processed@formula12345.dfs.core.windows.net/qualifying")

# qualifying_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

merge_condition = "tgt.qualifyId = src.qualifyId AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_file_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")