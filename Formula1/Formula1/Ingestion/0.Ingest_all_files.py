# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

vresult = dbutils.notebook.run("1.ingestion_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("2.assignment_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("3.Ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("4.Ingest_drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("5.Ingest_lap_time_folder", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("6.Ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("7.Ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

vresult

# COMMAND ----------

vresult = dbutils.notebook.run("8.Ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

vresult

# COMMAND ----------

# %sql
# SELECT race_id, COUNT(1)
# FROM f1_processed.results
# GROUP BY race_id
# ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE f1_processed.circuits;
# MAGIC -- # # DROP TABLE f1_processed.races;
# MAGIC -- # # DROP TABLE f1_processed.results;
# MAGIC -- # # DROP TABLE f1_processed.drivers;
# MAGIC -- # # DROP TABLE f1_processed.constructors;
# MAGIC -- # # DROP TABLE f1_processed.lap_times;
# MAGIC -- # # DROP TABLE f1_processed.pit_stops;
# MAGIC -- # # DROP TABLE f1_processed.qualifying;

# COMMAND ----------

