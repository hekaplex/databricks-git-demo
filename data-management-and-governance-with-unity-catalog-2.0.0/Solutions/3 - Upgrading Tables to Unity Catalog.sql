-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Upgrading Tables to Unity Catalog
-- MAGIC
-- MAGIC In this demo, you will learn essential techniques for upgrading tables to the Unity Catalog, a pivotal step in efficient data management. This demo will cover various aspects, including analyzing existing data structures, applying migration techniques, evaluating transformation options, and upgrading metadata without moving data. Both SQL commands and user interface (UI) tools will be utilized for seamless upgrades.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC 1. Analyze the current catalog, schema, and table structures in your data environment.
-- MAGIC 2. Execute methods to move data from Hive metastore to Unity Catalog, including cloning and Create Table As Select \(CTAS\).
-- MAGIC 3. Assess and apply necessary data transformations during the migration process.
-- MAGIC 4. Utilize methods to upgrade table metadata while keeping data in its original location.
-- MAGIC 5. Perform table upgrades using both SQL commands and user interface tools for efficient data management.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC In order to follow along with this demo, you will need:
-- MAGIC * Account administrator capabilities
-- MAGIC * Cloud resources to support the metastore
-- MAGIC * Have metastore admin capability in order to create and manage a catalog
-- MAGIC * **`CREATE CATALOG`** privileges in order to create a catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tasks to Perform
-- MAGIC
-- MAGIC As part of this demo, you are required to perform the following tasks:
-- MAGIC
-- MAGIC * **Task 1:** Analyze Data Objects in Classroom Setup
-- MAGIC   * 1.1 Analyze the Current Catalog
-- MAGIC   * 1.2 Analyze the Current Schema
-- MAGIC   * 1.3. Analyze the List of Available Tables and Views in the Custom Schema
-- MAGIC   * 1.4. Explore the Source Table
-- MAGIC
-- MAGIC * **Task 2:** Implement Upgrade Methods
-- MAGIC   * 2.1 Moving Table Data into the Unity Catalog Metastore
-- MAGIC       * Cloning a Table
-- MAGIC       * Create Table As Select (CTAS)
-- MAGIC       * Applying Transformations during the Upgrade
-- MAGIC   * 2.2 Upgrading Metadata Only
-- MAGIC       * Using SYNC
-- MAGIC       * Using the Data Explorer
-- MAGIC
-- MAGIC Let us now implement the above tasks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC Please review the following requirements before starting the lesson:
-- MAGIC
-- MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **14.3.x-scala2.12 14.3.x-photon-scala2.12 14.3.x-cpu-ml-scala2.12**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly. This will also create an example source table called *movies* within the legacy Hive metastore.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-new-03

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Note:** We have created a custom schema named **example** and have set it up as the current schema as part of the classroom setup.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.3 Analyze the List of Available Table and Views in the Custom Schema
-- MAGIC Let us analyze the custom schema for the list of tables and views.

-- COMMAND ----------

-- Show the list of tables within the custom schema
SHOW TABLES FROM example;

-- COMMAND ----------

-- Show the list of views within the custom schema
SHOW VIEWS FROM example;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC According to the above observation, the custom schema **does not yet contain any tables or views** because the two SQL queries above produced no results.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.4 Exploring the Source Table
-- MAGIC
-- MAGIC As part of the setup, we now have a table called *movies*, residing in a user-specific schema of the Hive metastore. To make things easier, the schema name is stored in a variable named **`clean_username`**. Let's preview the data stored in this table using that variable. Notice how the three-level namespaces makes referencing data objects in the Hive metastore seamless.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #  Show the first 10 rows from the movies table residing in the user-specific schema of the Hive metastore
-- MAGIC display(spark.sql(f"SELECT * FROM hive_metastore.{clean_username}.movies LIMIT 10"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 2: Overview of Upgrade Methods
-- MAGIC
-- MAGIC There are a few different ways to upgrade a table, but the method you choose will be driven primarily by how you want to treat the table data. If you wish to leave the table data in place, then the resulting upgraded table will be an external table. If you wish to move the table data into your Unity Catalog metastore, then the resulting table will be a managed table. Consult [this page](https://docs.databricks.com/en/data-governance/unity-catalog/index.html#managed-versus-external-tables-and-volumes) for tips on whether to choose a managed or external table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Moving Table Data into the Unity Catalog Metastore
-- MAGIC
-- MAGIC In this approach, table data will be copied from wherever it resides into the managed data storage area for the destination schema, catalog or metastore. The result will be a managed Delta table in your Unity Catalog metastore. 
-- MAGIC
-- MAGIC This approach has two main advantages:
-- MAGIC * Managed tables in Unity Catalog can benefit from product optimization features that may not work well (if at all) on tables that aren't managed
-- MAGIC * Moving the data also gives you the opportunity to restructure your tables, in case you want to make any changes
-- MAGIC
-- MAGIC The main disadvantage to this approach is, particularly for large datasets, the time and cost associated with copying the data.
-- MAGIC
-- MAGIC In this section, we cover two different options that will move table data into the Unity Catalog metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1.1 Cloning a Table
-- MAGIC
-- MAGIC Cloning a table is optimal when the source table is Delta (see <a href="https://docs.databricks.com/delta/clone.html" target="_blank">documentation</a> for a full explanation). It's simple to use, it will copy metadata, and it gives you the option of copying data (deep clone) or optionally leaving it in place (shallow clone). Shallow clones can be useful in some use cases.
-- MAGIC
-- MAGIC Run the following cell to check the format of the source table.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Describe the properties of the "movies" table in the user-specific schema of the Hive metastore using the extended option for more details.
-- MAGIC display(spark.sql(f"DESCRIBE EXTENDED hive_metastore.{clean_username}.movies"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Referring to the *Provider*, we see the source is a Delta table. So let's perform a deep clone operation to copy the table, creating a destination table named *movies_clone*.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #  Deep clone the "movies" table from the user-specific schema of the Hive metastore to create a new table named "movies_clone" in the user-specific catalog of the example schema.
-- MAGIC spark.sql(f"CREATE OR REPLACE TABLE {clean_username}.example.movies_clone CLONE hive_metastore.{clean_username}.movies")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1.2 Create Table As Select (CTAS)
-- MAGIC
-- MAGIC Using CTAS is a universally applicable technique that simply creates a new table based on the output of a **`SELECT`** statement. This will always copy the data, and no metadata will be copied.
-- MAGIC
-- MAGIC Let's copy the table using this approach, creating a destination table named *movies_ctas*.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Copy the "movies" table from the user-specific schema of the Hive metastore to create "movies_ctas" in the user-specific catalog's example schema using CTAS (Create Table As Select)
-- MAGIC spark.sql(f"""CREATE OR REPLACE TABLE {clean_username}.example.movies_ctas
-- MAGIC   AS SELECT * FROM hive_metastore.{clean_username}.movies""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.1.3 Applying Transformations during the Upgrade
-- MAGIC
-- MAGIC CTAS offers an option that other methods do not: the ability to transform the data while copying it.
-- MAGIC
-- MAGIC When migrating your tables to Unity Catalog, it's a great time to consider your table structures and whether they still address your organization's business requirements that may have changed over time.
-- MAGIC
-- MAGIC Cloning, and the CTAS operation we just saw, takes an exact copy of the source table. But CTAS can be easily adapted to perform any transformations during the upgrade. For example, let's expand on the previous example to do the following transformations:
-- MAGIC * Assign the name *idx* to the first column
-- MAGIC * Select only the columns *title*, *year*, *budget* and *rating*
-- MAGIC * Convert *year* and *budget* to **INT** (replacing any instances of the string *NA* with 0)
-- MAGIC * Convert *rating* to **DOUBLE**
-- MAGIC
-- MAGIC Since **`SELECT`** is such a versatile operation, there are many other options not mentioned here. For example, you could drop records that no longer seem relevant, enrich your tables by joining them with other lookup tables, and much more.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Copy the "movies" table from Hive metastore to create "movies_transformed" in the user-specific catalog using CTAS with the required transformations
-- MAGIC spark.sql(f"""CREATE OR REPLACE TABLE {clean_username}.example.movies_transformed
-- MAGIC AS SELECT
-- MAGIC   _c0 AS idx,
-- MAGIC   title,
-- MAGIC   CAST(year AS INT) AS year,
-- MAGIC   CASE WHEN
-- MAGIC     budget = 'NA' THEN 0
-- MAGIC     ELSE CAST(budget AS INT)
-- MAGIC   END AS budget,
-- MAGIC   CAST(rating AS DOUBLE) AS rating
-- MAGIC FROM hive_metastore.{clean_username}.movies""")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Display the contents of the "movies_transformed" table from the user-specific catalog of the example schema
-- MAGIC display(spark.sql(f"""SELECT * FROM {clean_username}.example.movies_transformed"""))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### 2.2 Upgrading Metadata Only
-- MAGIC
-- MAGIC We have seen approaches that involve moving table data from wherever it is currently to the Unity Catalog metastore. However, in upgrading external tables, some use cases may call for leaving the data in place. For example:
-- MAGIC * Data location is dictated by an internal or regulatory requirement of some sort
-- MAGIC * Cannot change the data format to Delta
-- MAGIC * Outside writers must be able to modify the data
-- MAGIC * Avoiding time and/or cost of moving large datasets
-- MAGIC
-- MAGIC Note the following constraints for this approach:
-- MAGIC
-- MAGIC * Source table must be an external table
-- MAGIC * There must be a storage credential referencing the storage container where the source table data resides
-- MAGIC
-- MAGIC In this section, we cover two different options that will upgrade to an external table without moving any table data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.2.1 Using SYNC
-- MAGIC
-- MAGIC The **`SYNC`** SQL command allows us to upgrade tables or entire schemas from the Hive metastore to Unity Catalog. Note that we're including the **`DRY RUN`** option, which assesses feasibility and doesn't actually perform any actions. This is never a bad idea the first time we attempt to perform this operation.
-- MAGIC
-- MAGIC Refer to the **`description`** column for an explanation of whether or not the operation would succeed and why.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Perform a SYNC operation with the DRY RUN option on the "test_external" table from the user-specific schema of the Hive metastore to create the same table in the current catalog's user-specific schema.
-- MAGIC display(spark.sql(f"""SYNC TABLE {clean_username}.example.test_external FROM hive_metastore.{clean_username}.test_external DRY RUN"""))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC There are a number of conditions that could cause this operation to fail. In this case, source table data resides in DBFS. This is not an acceptable location since files in DBFS cannot be protected through Unity Catalog. Locating data files there could therefore provide a way to bypass Unity Catalog security.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.2.2 Using the Data Explorer
-- MAGIC
-- MAGIC Let's try upgrading the table using the Catalog Explorer user interface.
-- MAGIC
-- MAGIC 1. Open the **Catalog Explorer**.
-- MAGIC 1. Select the **hive_metastore** catalog, then select your schema containing the source table.
-- MAGIC 1. Select the **movies** table \(it can be any available table\).
-- MAGIC 1. Click **Upgrade**.
-- MAGIC 1. Select your destination catalog and schema. Here, select your _**current catalog**_ and the custom schema **example** that was created by the classroom setup.
-- MAGIC 1. For this example, let's leave owner set to the default \(your username\) and click **Next**.
-- MAGIC 1. From here you can run the upgrade, or open a notebook containing the upgrade operations that you can run interactively. For the purpose of the exercise, you don't need to actually run the upgrade though.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC In this demo, we explored crucial techniques for upgrading tables to the Unity Catalog, focusing on efficient data management. We learned to analyze existing data structures, apply migration techniques, evaluate transformation options, and upgrade metadata without moving data. Through SQL commands and user interface tools, we seamlessly executed upgrades, considering the treatment of table data as either external or managed within the Unity Catalog. With a thorough understanding of these methods, you are now equipped to optimize your data management processes effectively.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
