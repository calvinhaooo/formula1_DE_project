# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the races data
# MAGIC #### 1. Read the races csv file via spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

fomula1dl_account_key = dbutils.secrets.get(scope = 'formula12345-scope', key = 'formula12345-account-key')

spark.conf.set(
    "fs.azure.account.key.formula12345.dfs.core.windows.net",
    fomula1dl_account_key
)

display(dbutils.fs.ls("abfss://raw@formula12345.dfs.core.windows.net"))

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source", "Ergast API")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

races_schema = StructType([
  StructField("raceId", IntegerType(), False),
  StructField("year", IntegerType(), True),
  StructField("round", IntegerType(), True),
  StructField("circuitId", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date", DateType(), True),
  StructField("time", StringType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv(f"abfss://raw@formula12345.dfs.core.windows.net/{v_file_data}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Select the specific data columns and merge two time columns

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, concat, lit, to_timestamp

# COMMAND ----------

races_add_df = races_df.withColumn("ingestion_date", current_timestamp())\
                       .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" ") , col("time")), "yyyy-MM-dd HH:mm:ss"))\
                       .withColumn("data_source", lit(v_data_source))\
                       .withColumn("file_date", lit(v_file_data))

# COMMAND ----------

display(races_add_df)

# COMMAND ----------

select_races_df = races_add_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"),col("data_source"),col("file_date"), col("race_timestamp"))

# COMMAND ----------

display(select_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write data to datalake as parquet

# COMMAND ----------

# select_races_df.write.mode("append").parquet(f"{processed_file_path}/races")
select_races_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM f1_processed.races where file_date = '2021-03-28';
# MAGIC -- REFRESH TABLE f1_processed.races

# COMMAND ----------

# display(spark.read.parquet("abfss://processed@formula12345.dfs.core.windows.net/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")