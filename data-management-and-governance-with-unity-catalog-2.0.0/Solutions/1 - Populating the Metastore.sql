-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Populating the Metastore
-- MAGIC In this demo, we will populate the metastore, focusing on the three-level namespace concept to create data containers and objects. We will cover the creation and management of catalogs, schemas, tables, views, and user-defined functions, demonstrating the execution of SQL commands to achieve these tasks. Additionally, we will verify the settings and inspect the data objects to ensure they are correctly stored and accessible for further analysis. This process includes creating and populating a managed table and view, as well as defining and executing a user-defined function to enhance SQL capabilities within the Unity Catalog environment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC 1. Identify the prerequisites and understand the three-level namespace concept introduced by Unity Catalog.
-- MAGIC 2. Describe the process of creating and managing catalogs, schemas, tables, views, and user-defined functions within Unity Catalog.
-- MAGIC 3. Execute SQL commands to create and manage catalogs, schemas, tables, and views, and implement user-defined functions in the Unity Catalog environment.
-- MAGIC 4. Inspect and verify the current catalog and schema settings, ensuring that the objects created are correctly stored and accessible for further analysis.
-- MAGIC 5. Develop and populate a managed table and a view, as well as define and execute a user-defined function to extend SQL capabilities within the Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-new-01

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Three-Level Namespace Recap
-- MAGIC
-- MAGIC Anyone with SQL experience will likely be familiar with the traditional two-level namespace to address tables or views within a schema (often referred to as a database) as shown in the following example:
-- MAGIC
-- MAGIC ```sql
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC ```
-- MAGIC Unity Catalog introduces the concept of a **catalog** into the hierarchy, which provides another containment layer above the schema layer. This provides a new way for organizations to segregate their data and can be handy in many use cases. For example:
-- MAGIC
-- MAGIC * Separating data relating to business units within your organization (sales, marketing, human resources, etc)
-- MAGIC * Satisfying SDLC requirements (dev, staging, prod, etc)
-- MAGIC * Establishing sandboxes containing temporary datasets for internal use
-- MAGIC
-- MAGIC You can have as many catalogs as you want in the metastore, and each can contain as many schemas as you want. To deal with this additional level, complete table/view references in Unity Catalog use a three-level namespace, like this:
-- MAGIC
-- MAGIC ```sql
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC ```
-- MAGIC We can take advantage of the **`USE`** statements to select a default catalog or schema to make our queries easier to write and read:
-- MAGIC
-- MAGIC ```sql
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Data Containers and Objects to Populate the Metastore
-- MAGIC
-- MAGIC In this section, let's explore how to create data containers and objects in the metastore. This can be done using SQL. Note that the SQL statements used throughout this lab could also be applied in DBSQL as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Catalog
-- MAGIC
-- MAGIC We'll begin by creating a new catalog, which represents the first component of the three-level namespace.
-- MAGIC
-- MAGIC In this command, we are using python because the lab environment requires a unique name for the catalog, and python is the easiest way to use a variable. We will use the variable **`clean_username`**, which was created by the classroom setup script we ran at the beginning of this lesson and is simply our current username with all non-alphanumeric characters removed.
-- MAGIC
-- MAGIC This variable provides a name that is guaranteed to be unique in the metastore.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a new catalog
-- MAGIC display(spark.sql(f"CREATE CATALOG IF NOT EXISTS {clean_username}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Selecting a Default Catalog
-- MAGIC
-- MAGIC Let's select the newly created catalog as the default. Now, any schema references you make will be assumed to be in this catalog unless you explicitly select a different catalog using a three-level specification.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Selecting a default catalog
-- MAGIC display(spark.sql(f"USE CATALOG {clean_username}"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verify the Current Catalog
-- MAGIC Incidentally, we can always inspect the current catalog setting using the **`current_catalog()`** function in SQL.

-- COMMAND ----------

-- Verify the current catalog
SELECT current_catalog()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating and Using a Schema
-- MAGIC In the following code cell, we will:
-- MAGIC * Create a schema
-- MAGIC * Select the schema as the default schema
-- MAGIC * Verify that our schema is, indeed, the default
-- MAGIC
-- MAGIC The concept of a schema in Unity Catalog is similar to the schemas or databases with which you're already likely familiar. Schemas contain data objects like tables and views, but can also contain functions and ML/AI models. Let's create a schema in the catalog we just created. We don't have to worry about name uniqueness since our new catalog represents a clean namespace for the schemas it can hold.
-- MAGIC

-- COMMAND ----------

-- Create a new schema
CREATE SCHEMA IF NOT EXISTS example;

-- Make our newly created schema the default
USE SCHEMA example;

-- Verify the current default schema
SELECT current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Managed Table
-- MAGIC With all the necessary containers in place, let's turn our attention to creating some data objects.
-- MAGIC
-- MAGIC First let's create a table. For this example, we'll pre-populate the table with a simple mock dataset containing synthetic patient heart rate data.
-- MAGIC
-- MAGIC Note the following:
-- MAGIC * We only need to specify the table name when creating the table. We don't need to specify the containing catalog or schema because we already selected defaults earlier with **`USE`** statements.
-- MAGIC * This will be a managed table, since we aren't specifying a **`LOCATION`**.
-- MAGIC * Because it's a managed table, it will also be a Delta table.

-- COMMAND ----------

-- Create the silver table
CREATE OR REPLACE TABLE silver
(
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Populate the Managed Table
-- MAGIC Populate the *silver* table with some sample data.

-- COMMAND ----------

INSERT INTO silver VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558),
  (23,'40580129',NULL,'2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177',NULL,'2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842',NULL,'2020-02-01T00:08:58.000+0000',1000052.1354807863),
  (23,'40580129',NULL,'2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',9),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',7),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',6),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',5),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',9000),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',66),
  (23,'40580129',NULL,'2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177',NULL,'2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842',NULL,'2020-02-01T00:08:58.000+0000',1000052.1354807863),
  (23,'40580129',NULL,'2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',98),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',90),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',60),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',50),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',30),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',80);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query the Table
-- MAGIC Execute queries on the created table to examine its contents, ensuring that data is correctly stored and accessible for analysis purposes.

-- COMMAND ----------

-- View the rows inside the silver table
SELECT * FROM silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating and Managing Views
-- MAGIC
-- MAGIC Let's create a *gold* view that presents a processed view of the *silver* table data by averaging heart rate data per patient on a daily basis.
-- MAGIC In the following cell, we:
-- MAGIC * Create the view
-- MAGIC * Query the view

-- COMMAND ----------

-- Create a gold view
CREATE OR REPLACE VIEW gold AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time));
-- View the rows inside the gold view
SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating and Managing User-Defined Functions
-- MAGIC A User-Defined Function (UDF) in SQL is a feature that allows you to extend the native capabilities of SQL. It enables you to define your business logic as reusable functions that extend the vocabulary of SQL, for transforming or masking data, and reuse it across your applications. User-defined functions are contained by schemas as well. For this example, we'll set up simple function that masks all but the last two characters of a string.

-- COMMAND ----------

-- Create a custom function to mask a string value
CREATE OR REPLACE FUNCTION dbacademy_mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(LEFT(x, 2) , REPEAT("*", LENGTH(x) - 2))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Executing the User-Defined Function
-- MAGIC Let's see how the function we just defined works. Note that you can expand the table by dragging the border to the right if the output is truncated.

-- COMMAND ----------

-- Run the custom function to verify its output
SELECT dbacademy_mask('sensitive data') AS data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using the Catalog Explorer
-- MAGIC All of the data objects we have created are available to us in the Data Explorer. We can view and change permissions, change object ownership, examine lineage, and a whole lot more. Follow the instructions below:
-- MAGIC 1. Open the Catalog Explorer by right-clicking on **`Catalog`** in the left sidebar menu and opening the link in a new tab. This will allow you to keep these instructions open
-- MAGIC 1. Run the following cell to get your current catalog's name, and copy and paste the name in the *Type to filter* cell
-- MAGIC 1. Click on your catalog's name  
-- MAGIC
-- MAGIC You will see a list of four schemas in the catalog: 
-- MAGIC * default -- this schema is created when the catalog is created and can be dropped, if needed
-- MAGIC * dmguc -- created by the *Classroom Setup* script we ran at the beginning of the notebook (dmguc is short for "Data Management and Governance with Unity Catalog)
-- MAGIC * example -- we created this schema above
-- MAGIC * information_schema -- this schema is created when the catalog is created and contains a wealth of metadata. We will talk more about this schema in a future lesson
-- MAGIC
-- MAGIC Let's delete the *default* schema. Click the schema name to open its details page. In the upper-right corner, click the "kebab" menu, and select **`Delete`** to delete the schema. Click the warning to accept.
-- MAGIC
-- MAGIC Now, lets look at the *example* schema we created:
-- MAGIC 1. Click **`example`** to view the details for this schema
-- MAGIC 1. Note the two data objects in the schema, the *silver* table and the *gold* view. Click the *silver* table name
-- MAGIC 1. We see the columns we defined for this table. Note the button labeled *AI Generate*. We can use this to generate comments for our columns. Click *AI generate*
-- MAGIC 1. The Data Intelligence Platform proposes comments for each column in our table. Click the check next to the first comment to accept it
-- MAGIC 1. We also have an AI suggested description for the table. Click **`Accept`**  
-- MAGIC
-- MAGIC There are tabs along the top where we can view and manage metadata about this table:
-- MAGIC * Overview -- On the right, we have information about the table, tags, its description, and we can add a row filtering function if we wish. We will talk about this in a future lesson. On the left, we get information about each column in the table
-- MAGIC * Sample data -- This tab gives us the first rows of the table, so we can see the data contained within the table
-- MAGIC * Details -- We get the same information here as we would get by running **`DESCRIBE EXTENDED silver`**
-- MAGIC * Permissions -- The UI give us the ability to manage table permissions. We can **`GRANT`** and **`REVOKE`** permissions here. We will talk about doing this programmatically in a future lesson
-- MAGIC * History -- The Delta Lake transaction log is on this tab. We can get this programmatically by running **`DESCRIBE HISTORY silver`**
-- MAGIC * Lineage -- It is very helpful to see table lineage. Click **`See lineage graph`** to see both our *silver* table and the *gold* view. Note that the view gets its data from the *silver* table. Click the "X" in the upper-right corner to close the window
-- MAGIC * Insights -- The Databricks Data Intelligence Platform provides these insights, so we can see how our data object is being used.
-- MAGIC * Quality -- This tab gives us the ability to monitor this table for data quality. Let's talk more about this next

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(clean_username)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Databricks Lakehouse Monitoring
-- MAGIC Databricks Lakehouse Monitoring lets you monitor statistical properties and quality of the data in a particular table or all tables in your account. To monitor a table in Databricks, you create a monitor attached to the table.  
-- MAGIC
-- MAGIC We can create a monitor using the Catalog Explorer or the API. Let's use the Catalog Explorer to create a monitor attached to our **`silver`** table.
-- MAGIC
-- MAGIC If you aren't already on the **`Quality`** tab, click it. To create a new monitor, click **`Get Started`**. Drop down the **`Profile type`** and select **`Snapshot`**. Drop down **`Advanced options`**, and select **`Refresh manually`** for the refresh type.
-- MAGIC
-- MAGIC Click **`Create`**.
-- MAGIC
-- MAGIC **Note:** Creating a monitor can take a minute or two. Wait until the monitor is fully created before running the next cell.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC from databricks import lakehouse_monitoring as lm
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC w = WorkspaceClient()
-- MAGIC try:
-- MAGIC     current_refreshes = w.quality_monitors.list_refreshes(
-- MAGIC         table_name=f"{clean_username}.example.silver"
-- MAGIC     )
-- MAGIC     current_refresh_id = current_refreshes.refreshes[0].refresh_id
-- MAGIC     current_refresh = ""
-- MAGIC
-- MAGIC     current_refresh_state = ""
-- MAGIC     while current_refresh_state != "RefreshState.SUCCESS":
-- MAGIC         current_refresh_state = str(lm.get_refresh(table_name = f"{clean_username}.example.silver", refresh_id = f"{current_refresh_id}").state)
-- MAGIC         print(current_refresh_state)
-- MAGIC         time.sleep(20)
-- MAGIC except:
-- MAGIC     print("It appears that the monitor has not been created yet or it has not finished being created. Please wait to run this cell after the monitor has been created.")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Looking at the Monitor's Dashboard
-- MAGIC
-- MAGIC Once the initial update is complete, click **`View dashboard`** on the monitor page to open the monitor's example dashboard. Since we have been using sample data, we need to change the dropdowns for **`Start Time`** and **`End Time`** to fit the dates in the data. 
-- MAGIC
-- MAGIC 1. Drop down **`Start Time`** by clicking on the number 12 (not the calendar icon) in the dropdown field. 
-- MAGIC 1. Click the double, left-pointing arrow until you reach "2019." 
-- MAGIC 1. Select the first day of any month in 2019.
-- MAGIC 1. Click **`Ok`**
-- MAGIC 1. Click the calendar icon in the **`End Time`** field, and select **`Now`**.
-- MAGIC 1. From the top right corner, select an available **warehouse**.
-- MAGIC 1. Click **`Ok`**.  
-- MAGIC   
-- MAGIC The dashboard runs a refresh. When the refresh completes, note some of the visualizations in the dashboard.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Add More Data
-- MAGIC Run the following two cells to add more data to the **`silver`** table. Then, run the next table to start a monitor refresh (this one should not take as long as the first one).

-- COMMAND ----------

INSERT INTO silver VALUES
  (23,'40580129','Nicholas Spears','2024-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2024-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2024-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2024-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2024-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2024-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2024-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2024-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2024-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2024-02-01T00:46:57.000+0000',54.8372685558),
  (23,'40580129',NULL,'2024-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177',NULL,'2024-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842',NULL,'2024-02-01T00:08:58.000+0000',1000052.1354807863),
  (23,'40580129',NULL,'2024-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2024-02-01T00:18:08.000+0000',0),
  (37,'65300842','Samuel Hughes','2024-02-01T00:23:58.000+0000',0),
  (23,'40580129','Nicholas Spears','2024-02-01T00:31:58.000+0000',0),
  (17,'52804177','Lynn Russell','2024-02-01T00:32:56.000+0000',0),
  (37,'65300842','Samuel Hughes','2024-02-01T00:38:54.000+0000',0),
  (23,'40580129','Nicholas Spears','2024-02-01T00:46:57.000+0000',0),
  (23,'40580129',NULL,'2024-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177',NULL,'2024-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842',NULL,'2024-02-01T00:08:58.000+0000',1000052.1354807863),
  (23,'40580129',NULL,'2024-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2024-02-01T00:18:08.000+0000',0),
  (37,'65300842','Samuel Hughes','2024-02-01T00:23:58.000+0000',0),
  (23,'40580129','Nicholas Spears','2024-02-01T00:31:58.000+0000',0),
  (17,'52804177','Lynn Russell','2024-02-01T00:32:56.000+0000',0),
  (37,'65300842','Samuel Hughes','2024-02-01T00:38:54.000+0000',0),
  (23,'40580129','Nicholas Spears','2024-02-01T00:46:57.000+0000',0);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dropping a Managed Table
-- MAGIC Because the table we created is a managed table, when it is dropped, the data we added to it is also deleted.

-- COMMAND ----------

DROP TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop a schema
-- MAGIC We have to add **`CASCADE`** because the schema is not empty.

-- COMMAND ----------

DROP SCHEMA example CASCADE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC In this demo, we explored the process of upgrading tables to Unity Catalog, focusing on the three-level namespace concept. We created and managed catalogs, schemas, tables, views, and user-defined functions, demonstrating the execution of SQL commands for each task. We ensured correct storage and accessibility of data objects, created and populated a managed table and view, and defined a user-defined function to enhance SQL capabilities within the Unity Catalog environment. This comprehensive approach provided a structured and efficient method to manage data in the metastore, leveraging Unity Catalog's advanced features.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
