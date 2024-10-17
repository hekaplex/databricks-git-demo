# Databricks notebook source
# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

course_code = "dmguc"
spark.conf.set("course_code", course_code)

# COMMAND ----------

# MAGIC %run "./1 - Dataset Listing"

# COMMAND ----------

# Create catalog, schema, and volume (for datasets)
import re
import os, io
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import col

w = WorkspaceClient()

# Get the username
username = w.current_user.me().user_name
spark.conf.set("username", username)

# Clean username for catalog name
clean_username = re.sub("[^A-Za-z0-9_]", "", username)
spark.conf.set("clean_username", clean_username)

# Check if catalog exists and create if needed
catalog_exists = spark.sql(f"SHOW CATALOGS LIKE '{clean_username}'")
if catalog_exists.select(col("catalog")).first() == None:
  print(f"Creating catalog:               {clean_username}")
  spark.sql(f"CREATE CATALOG {clean_username};")
else:
  print("Found an existing catalog for this user.")
spark.sql(f"USE CATALOG {clean_username};")

# Check if schema exists and create if needed
if spark.catalog.databaseExists(course_code) == False:
  print(f"Creating schema:                {course_code}")
  spark.sql(f"CREATE SCHEMA {course_code};")
else:
  print("Found an existing schema for this course.")
spark.sql(f"USE SCHEMA {course_code};")

# Check if data volume exists and create if needed
volume_exists = spark.sql(f"SHOW VOLUMES IN {course_code} LIKE 'data'")
if volume_exists.select(col("volume_name")).first() == None:
  print(f"Creating data storage volume:   data")
  spark.sql(f"CREATE VOLUME data;")
else:
  print("Found an existing data volume for this course.")

# Import datasets into the data volume if needed
print(f"\nImporting datasets. This may take some time.")
import requests
from pathlib import Path
existing_dataset_counter = 0
dataset_loading_counter = 0
for current_dataset_file_path in datasets:
  # Check if the dataset has already been loaded
  local_file_path = Path(f"/Volumes/{clean_username}/{course_code}/data/{current_dataset_file_path}")
  if local_file_path.exists():
    existing_dataset_counter += 1
  else:
    response = requests.get(dataset_path + current_dataset_file_path)
    binary_data = io.BytesIO(response.content)
    w.files.upload(f"/Volumes/{clean_username}/{course_code}/data{current_dataset_file_path}", binary_data, overwrite = True)
    dataset_loading_counter += 1
print(f"Found {existing_dataset_counter} existing datasets.")
print(f"Downloaded {dataset_loading_counter} new datasets.")
print("Done.")
