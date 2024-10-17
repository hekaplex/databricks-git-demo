-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Navigating the Metastore
-- MAGIC In this demo, we'll explore the structure and functionality of a metastore, delving into its various components like catalogs, schemas, and tables. We'll employ SQL commands such as SHOW and DESCRIBE to inspect and analyze these elements, enhancing our understanding of the metastore's configuration and the properties of different data objects. Additionally, we'll examine the roles of system catalogs and information_schema in metadata management, and highlight the importance of data lineage in data governance. This hands-on demonstration will equip participants with the knowledge to effectively navigate and utilize metastores in a cloud environment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC 1. Discuss the structure and function of a metastore, including its different components such as catalogs, schemas, and tables.
-- MAGIC 2. Apply SQL commands like **SHOW** and **DESCRIBE** to inspect and explore different elements within the metastore, such as catalogs, schemas, tables, user-defined functions, and privileges.
-- MAGIC 3. Analyze and interpret the configuration of the metastore and the properties of various data objects.
-- MAGIC 4. Evaluate the roles of the system catalog and the information_schema in managing and accessing metadata.
-- MAGIC 5. Identify and explain the importance of data lineage as part of data governance.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Prerequisites
-- MAGIC In order to follow along with this demo, you will need:
-- MAGIC * **`CREATE CATALOG`** privileges in order to create a catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-new-02

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Note:** We have created a custom schema named **example** and have set it up as the current schema as part of the classroom setup.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analyze Data Objects in Classroom Setup
-- MAGIC Let us analyze the current data objects and their components during the classroom setup.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze the Current Catalog
-- MAGIC Let us analyze the current catalog.

-- COMMAND ----------

-- Verify the curent catalog
SELECT current_catalog()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As per the above observation, the current catalog is the **default catalog** created with the classroom setup.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze the Current Schema
-- MAGIC Let us analyze the current schema.

-- COMMAND ----------

-- Verify the curent schema
SELECT current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As per the above observation, the current schema is the **custom schema** created with the name **'example'**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze the List of Available Tables and Views in the Custom Schema
-- MAGIC Let us analyze the custom schema for the list of tables and views.

-- COMMAND ----------

SHOW TABLES FROM example;

-- COMMAND ----------

SHOW VIEWS FROM example;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that **`SHOW TABLES`** will display both tables and views, and **`SHOW VIEWS`** will only show views. From the above observation, there are the following tables and views in the custom schema:
-- MAGIC 1. Table\(s\): silver
-- MAGIC 2. View\(s\): gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Exploring the Metastore
-- MAGIC
-- MAGIC In this section, let's explore our metastore and its data objects.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL
-- MAGIC Let's explore objects using the SQL commands. Though we embed them in a notebook here, you can easily port them over for execution in the DBSQL environment as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Inspect Elements with SQL `SHOW` Command
-- MAGIC
-- MAGIC We use the SQL **`SHOW`** command to inspect elements at various levels in the hierarchy.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inspect Catalogs
-- MAGIC Let's start by taking stock of the catalogs in the metastore.

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Do any of these entries surprise you? You should definitely see a catalog beginning with your user name as the prefix, which is the one we created earlier. But there may be more, depending on the activity in your metastore, and how your workspace is configured. In addition to catalogs others have created, you will also see some special catalogs:
-- MAGIC * *hive_metastore*. This is not actually a catalog. Rather, it's Unity Catalog's way of making the workspace local Hive metastore seamlessly accessible through the three-level namespace.
-- MAGIC * *main*: this catalog is created by default with each new metastore, though you can remove it from your metastore if desired (it isn't used by the system)
-- MAGIC * *samples*: this references a cloud container containing sample datasets hosted by Databricks.
-- MAGIC * *system*: this catalog provides an interface to the system tables - a collection of tables that return information about objects across all catalogs in the metastore.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inspect Schemas
-- MAGIC Now let's take a look at the schemas contained in the catalog we created. Remember that we have a default catalog selected so we needn't specify it in our query.

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The *example* schema, of course, is the one we created earlier but there are a couple additional entries you maybe weren't expecting:
-- MAGIC * *default*: this schema is created by default with each new catalog.
-- MAGIC * *information_schema*: this schema is also created by default with each new catalog and provides a set of views describing the objects in the catalog.
-- MAGIC
-- MAGIC As a sidenote, if we want to inspect schemas in a catalog that isn't the default, we specify it as follows.

-- COMMAND ----------

SHOW SCHEMAS IN samples

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inspect Tables
-- MAGIC Now let's take a look at the tables contained in the schema we created. Again, we don't need to specify schema or catalog since we're referring the defaults.

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC But, if we want to inspect elsewhere, we can explicitly override the default catalog and schema as follows.

-- COMMAND ----------

SHOW TABLES IN samples.tpch

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inspect User-Defined Functions
-- MAGIC There's a command available for exploring all the different object types. For example, we can display the user-defined functions.

-- COMMAND ----------

SHOW USER FUNCTIONS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inspect Privileges Granted on Data Objects
-- MAGIC We can also use **`SHOW`** to see privileges granted on data objects.

-- COMMAND ----------

SHOW GRANTS ON TABLE silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Since there are no grants on this table yet, no results are returned. That means that only you, the data owner, can access this table. We'll get to granting privileges shortly.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze Additional Information with SQL `DESCRIBE` Command
-- MAGIC
-- MAGIC We also have **`DESCRIBE`** at our disposal, to provide additional information about a specific object.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Analyze Tables
-- MAGIC Let us analyze the information about a few tables.

-- COMMAND ----------

DESCRIBE TABLE EXTENDED silver

-- COMMAND ----------

DESCRIBE TABLE EXTENDED gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Analyze User-Defined Functions
-- MAGIC Let us analyze the information about a user-defined function.

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED dbacademy_mask

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Analyze Other Data Objects
-- MAGIC We can also analyze other data objects in the metastore.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### System Catalog
-- MAGIC The *system* catalog provides an interface to the system tables; that is a collection of views whose purpose is to provide a SQL-based, self-describing API to the metadata related to objects across all catalogs in the metastore. This exposes a host of information useful for administration and housekeeping and there are a lot of applications for this.
-- MAGIC
-- MAGIC Let's consider the following example, which shows all tables that have been modified in the last 24 hours.
-- MAGIC
-- MAGIC In addition to demonstrating how to leverage this information, the query also demonstrates a Unity Catalog three-level namespace reference.

-- COMMAND ----------

SELECT table_name, table_owner, created_by, last_altered, last_altered_by, table_catalog
    FROM system.information_schema.tables
    WHERE  datediff(now(), last_altered) < 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Information Schema
-- MAGIC
-- MAGIC The *information_schema*, automatically created with each catalog, contains a collection of views whose purpose is to provide a SQL-based, self-describing API to the metadata related to the elements contained within the catalog.
-- MAGIC
-- MAGIC The relations found in this schema are documented <a href="https://docs.databricks.com/sql/language-manual/sql-ref-information-schema.html" target="_blank">here</a>. As a basic example, let's see all the tables in the catalog. Note that since we only specify two levels here, we're referncing the default catalog selected earlier.

-- COMMAND ----------

SELECT * FROM information_schema.tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Catalog Explorer
-- MAGIC Let's click the **Catalog** icon in the left sidebar to explore the metastore using the Catalog Explorer user interface.
-- MAGIC * Observe the catalogs listed in the **Catalog** pane. The items in this list resemble those from the **`SHOW CATALOGS`** SQL statement we executed earlier.
-- MAGIC * Expand the catalog we created, then expand *example*. This displays a list of tables/views.
-- MAGIC * Select *gold* to see detailed information regarding the view. From here, we can see the schema, sample data, details, and permissions (which we'll get to shortly).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lineage
-- MAGIC
-- MAGIC Data lineage is a key pillar of any data governance solution. In the **Lineage** tab, we can identify elements that are related to the selected object:
-- MAGIC * With **Upstream** selected, we see objects that gave rise to this object, or that this object uses. This is useful for tracing the source of your data.
-- MAGIC * With **Downstream** selected, we see objects that are using this object. This is useful for performing impact analyses.
-- MAGIC * The lineage graph provides a visualization of the lineage relationships.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC In this demo, we explored the structure and functionality of a metastore through practical exercises, enhancing our understanding of data organization and metadata management. We learned how to navigate and inspect various components such as catalogs, schemas, tables, and user-defined functions using SQL commands like SHOW and DESCRIBE. Additionally, we delved into the roles of the system catalog and information_schema, gaining insights into their importance in metadata access and management. The demo also highlighted the significance of data lineage for robust data governance, enabling us to trace data origins and impacts effectively. Overall, this hands-on approach has equipped us with essential skills to manage and analyze metadata within a metastore efficiently.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
