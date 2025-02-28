# Databricks notebook source
course_code = "DMGUC"
spark.conf.set("course_code", course_code)

# COMMAND ----------

# Create catalog, schema, and volume (for datasets)
import re
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
spark.conf.set("username", username)
clean_username = re.sub("[^A-Za-z0-9_]", "", username)
spark.conf.set("clean_username", clean_username)
print(f"Creating catalog:               {clean_username}")
spark.sql(f"DROP CATALOG IF EXISTS {clean_username} CASCADE;")
spark.sql(f"CREATE CATALOG {clean_username};")
spark.sql(f"USE CATALOG {clean_username};")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new schema
# MAGIC CREATE SCHEMA IF NOT EXISTS example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select a default schema
# MAGIC USE SCHEMA example

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the silver table
# MAGIC CREATE OR REPLACE TABLE silver
# MAGIC (
# MAGIC   device_id  INT,
# MAGIC   mrn        STRING,
# MAGIC   name       STRING,
# MAGIC   time       TIMESTAMP,
# MAGIC   heartrate  DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate the silver table with 10 rows
# MAGIC INSERT INTO silver VALUES
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
# MAGIC   (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
# MAGIC   (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
# MAGIC   (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a gold view
# MAGIC CREATE OR REPLACE VIEW gold AS (
# MAGIC   SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
# MAGIC   FROM silver
# MAGIC   GROUP BY mrn, name, DATE_TRUNC("DD", time))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a custom function to mask a string value
# MAGIC CREATE OR REPLACE FUNCTION dbacademy_mask(x STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT col1 AS `Object`,col2 AS `Name`
# MAGIC FROM VALUES
# MAGIC   ('Catalog (Default)','${DA.catalog_name}'),
# MAGIC   ('Schema (Custom)','example');
