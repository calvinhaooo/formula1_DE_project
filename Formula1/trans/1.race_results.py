# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funtions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", '2021-03-21')
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_file_path}/drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_file_path}/constructors")\
    .withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_file_path}/circuits")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_file_path}/races")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_file_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("time", "race_time")\
    .withColumnRenamed("race_id", "result_race_id")\
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC Join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC Join results to all other dataframe

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select('race_id','race_year', 'race_name', 'race_date', 'circuit_location','driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'result_file_date')\
    .withColumn('current_timestamp', current_timestamp())\
    .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_file_path}/race_results")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")
# overwrite_partition(final_df, "f1_presentation1", "race_results", "race_id")


# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_file_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

