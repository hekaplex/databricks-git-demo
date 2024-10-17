# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# import re

# class DBAcademyHelper():
#      def __init__(self):
#         import re
        
#         # Do not modify this pattern without also updating the Reset notebook.
#         username = spark.sql("SELECT current_user()").first()[0]
#         clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
#         self.catalog = f"dbacademy_{clean_username}"        
#         spark.conf.set("DA.catalog", self.catalog)

# DA = DBAcademyHelper()

# COMMAND ----------

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT col1 AS `Object`,col2 AS `Name`
# MAGIC FROM VALUES ('Catalog','${DA.catalog_name}')
