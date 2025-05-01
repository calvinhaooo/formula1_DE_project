# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC     FROM f1_presentation.calculated_race_results
# MAGIC   GROUP BY team_name
# MAGIC   HAVING COUNT(1) >= 100
# MAGIC   ORDER BY avg_points DESC

# COMMAND ----------

