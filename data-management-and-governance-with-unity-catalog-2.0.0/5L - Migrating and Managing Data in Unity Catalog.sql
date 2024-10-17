-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Migrating and Managing Data in Unity Catalog
-- MAGIC
-- MAGIC In this lab, you will migrate and manage data in Unity Catalog by analyzing the current data structure, ensuring the source table is in Delta format, and performing a deep clone of the "movies" table from Hive metastore to Unity Catalog as a managed table. You will then create a dynamic view with data redaction and access restrictions, grant access permissions, and validate the view's functionality.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC 1. Analyze the current catalog, schema, and data objects within the classroom setup to understand the existing data structure and organization.
-- MAGIC 2. Analyze the format of the source table to ensure it is in Delta format before proceeding with data migration.
-- MAGIC 3. Perform a deep clone of the source table to migrate it from the Hive metastore to the Unity Catalog metastore as a managed table.
-- MAGIC 4. Develop a dynamic view of the cloned table, applying data redaction and access restrictions to enhance data security.
-- MAGIC 5. Assign access permissions to the newly created view for account users and execute queries to validate the view's functionality and data integrity.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC In order to follow along with this lab, you will need:
-- MAGIC * Account administrator capabilities
-- MAGIC * Cloud resources to support the metastore
-- MAGIC * Have metastore admin capability in order to create and manage a catalog
-- MAGIC * **`CREATE CATALOG`** privileges in order to create a catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly. This will also create an example source table called *movies* within the legacy Hive metastore.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-new-03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.4 Exploring the Source Table
-- MAGIC
-- MAGIC As part of the setup, we now have a table called *movies*, residing in a user-specific schema of the Hive metastore. To make things easier, the schema name is stored in a Hive variable named **`DA.schema`**.
-- MAGIC
-- MAGIC Let's preview the data stored in this table using that variable. Notice how the three-level namespaces makes referencing data objects in the Hive metastore seamless.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # Show the first 10 rows from the movies table residing in the user-specific schema of the Hive metastore
-- MAGIC # <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 2: Moving Table Data into the Unity Catalog Metastore
-- MAGIC
-- MAGIC Transfer the "movies" table from the "examples" schema in "hive_metastore" to the "examples" schema in your catalog as a managed table using the clone method.
-- MAGIC
-- MAGIC Let's implement the clone method to transfer the "movies" table from "hive_metastore" to your current catalog as a managed table.
-- MAGIC
-- MAGIC To clone a table, ensure that the source table is in Delta format. Cloning is straightforward, preserving metadata, and offers the choice to either copy the data (deep clone) or leave it in its original location (shallow clone). Shallow clones may be beneficial in certain scenarios.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Verify the Source Table Format
-- MAGIC Execute the below cell to verify the source table format.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # Describe the properties of the "movies" table in the user-specific schema of the hive metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Referring to the row for *Provider*, we see the source is a Delta table. So we need to perform a deep clone operation to copy the table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Perform a Deep Clone to Copy the Table
-- MAGIC Perform a deep clone to copy the table creating a destination table named *movies_clone*.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO
-- MAGIC # Deep clone the "movies" table from the Hive metastore to create a new table named "movies_clone" in your current catalog.
-- MAGIC # <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.3 Query the Table
-- MAGIC Let us analyze the **movies_clone** table.

-- COMMAND ----------

-- Query the "movies_clone" table to see if it is populated.
-- <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 3: Protecting Columns and Rows with Dynamic Views
-- MAGIC Let's create a *movies_gold* view that presents a processed view of the *movies_clone* table data with the following transformations:
-- MAGIC 1. Redact the "votes" column data
-- MAGIC 2. Restrict the movies with a rating below 6.0
-- MAGIC 3. Order the data based on the moving rating in ascending order
-- MAGIC
-- MAGIC **Note:** You are required to have the following columns in your view:
-- MAGIC * _c0
-- MAGIC * title
-- MAGIC * year
-- MAGIC * length
-- MAGIC * budget
-- MAGIC * rating
-- MAGIC * view _\(this should be the **redacted view**\)_

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 Recreate the View
-- MAGIC Let us recreate a **movies_gold** view while redacting the "votes" column.

-- COMMAND ----------

-- Create a **movies_gold** view by redacting the "votes" column and restricting the movies with a rating below 6.0
-- <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 Issue Grant Access to View
-- MAGIC Let us issue a grant access for "account users" to view the **movies_gold** view.

-- COMMAND ----------

-- Grant VIEW access to `account users` to the movies_gold view
-- <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.3 Query the View
-- MAGIC Now let's query the view.

-- COMMAND ----------

-- Query the "movies_gold" view to see if it is populated.
-- <FILL_IN>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC In this lab, we thoroughly explored data migration and management within Unity Catalog. Initially, we analyzed the current data structure, ensuring the source table was in Delta format. Using the clone method, we transferred the "movies" table from Hive metastore to Unity Catalog. Then, we created the "movies_gold" dynamic view, implementing data redaction and access restrictions. This process has equipped you with skills to effectively analyze, migrate, and manage data in Unity Catalog while prioritizing integrity and security.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
