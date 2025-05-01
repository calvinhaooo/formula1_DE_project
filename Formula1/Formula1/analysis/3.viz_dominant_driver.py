# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dominant_drivers
# MAGIC AS
# MAGIC SELECT driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points,
# MAGIC        RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
# MAGIC     FROM f1_presentation.calculated_race_results
# MAGIC   GROUP BY driver_name
# MAGIC   HAVING COUNT(1) >= 50
# MAGIC   ORDER BY avg_points DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_dominant_drivers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year,
# MAGIC        driver_name,
# MAGIC        COUNT(1) AS total_races,
# MAGIC        SUM(calculated_points) AS total_points,
# MAGIC        AVG(calculated_points) AS avg_points
# MAGIC     FROM f1_presentation.calculated_race_results
# MAGIC     WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
# MAGIC   GROUP BY race_year, driver_name
# MAGIC   ORDER BY race_year, avg_points DESC

# COMMAND ----------

