# Databricks notebook source
# INSTALL_LIBRARIES
version = "v3.0.70"
if not version.startswith("v"): library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
else: library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"
pip_command = f"install --quiet --disable-pip-version-check {library_url}"

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %run ./_dataset_index

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig
from dbacademy.dbhelper.validations.validation_helper_class import ValidationHelper
from pyspark.sql.types import StructType

# The following attributes are externalized to make them easy
# for content developers to update with every new course.

course_config = CourseConfig(course_code = "asp",
                             course_name = "apache-spark-programming-with-databricks",
                             data_source_name = "apache-spark-programming-with-databricks",
                             data_source_version = "v03",
                             install_min_time = "2 min",
                             install_max_time = "5 min",
                             remote_files = remote_files,
                             supported_dbrs = ["13.3.x-scala2.12", "13.3.x-photon-scala2.12", "13.3.x-cpu-ml-scala2.12"],
                             expected_dbrs = "{{supported_dbrs}}")

lesson_config = LessonConfig(name = None,
                             create_schema = True,
                             create_catalog = False,
                             requires_uc = False,
                             installing_datasets = True,
                             enable_streaming_support = True,
                             enable_ml_support = False)
