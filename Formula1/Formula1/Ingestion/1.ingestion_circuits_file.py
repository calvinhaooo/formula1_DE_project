# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest the circuits data
# MAGIC #### 1. Read the circuits csv file via spark dataframe reader

# COMMAND ----------

fomula1dl_account_key = dbutils.secrets.get(scope = 'formula12345-scope', key = 'formula12345-account-key')

spark.conf.set(
    "fs.azure.account.key.formula12345.dfs.core.windows.net",
    fomula1dl_account_key
)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_funtions"

# COMMAND ----------

raw_file_path

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_data = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_data

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema = StructType([
  StructField("circuitId", IntegerType(), False),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", IntegerType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.schema(circuits_schema)\
.csv(f"abfss://raw@formula12345.dfs.core.windows.net/{v_file_data}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC From above, we can see the data types in the datafrome are all string attribute. Therefore, we should deal with this isse. Then, describe() can tell us some statistics of the dataset, then we can fix each variables. The Inferschema can fix this issue. --> not efficient
# MAGIC
# MAGIC Instead, we can create the schema for this dataset. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Select the specific data columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

select_circuits_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Rename the columns

# COMMAND ----------

circuits_renamed_df = select_circuits_df\
.withColumnRenamed("circuitId", "circuit_id")\
.withColumnRenamed("circuitRef", "circuit_ref")\
.withColumnRenamed("lat", "latitude")\
.withColumnRenamed("lng", "longtitude")\
.withColumnRenamed("alt", "altitude")\
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_data))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Add ingestion_date column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("append").parquet(f"{processed_file_path}/circuits")
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")