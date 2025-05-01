# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funtions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

races_result_list = spark.read.format("delta").load(f"{presentation_file_path}/race_results")\
    .filter(f"file_date = '{v_file_data}'")\
    .select("race_year")\
    .distinct()\
    .collect()

# COMMAND ----------

races_result_list

# COMMAND ----------

race_year_list = []
for race_year in races_result_list:
    race_year_list.append(race_year.race_year)
race_year_list

# COMMAND ----------

from pyspark.sql.functions import col
races_result_df = spark.read.format("delta").load(f"{presentation_file_path}/race_results")\
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standing_df = races_result_df\
    .groupby('race_year', 'driver_name', 'driver_nationality') \
    .agg(sum("points").alias("total_points"),
        count(when(col('position') == 1 , True)).alias('wins'))

# COMMAND ----------

from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_file_path}/driver_standings")
# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# overwrite_partition(final_df, "f1_presentation1", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_file_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM f1_presentation.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC FROM f1_presentation.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;

# COMMAND ----------

