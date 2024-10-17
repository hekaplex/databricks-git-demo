-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Controlling Access to Data
-- MAGIC In this demo, we explore the capabilities of Databricks' metastore, focusing on fine-grained access control through column masking, row filtering, and dynamic views. We will learn to analyze the structure and components of the metastore, implement SQL queries to examine catalogs, schemas, tables, and views, and control access to data objects. Through practical exercises, we will delve into techniques such as column masking to obscure sensitive information, row filtering to selectively retrieve data based on criteria, and dynamic views for conditional access control.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC 1. Analyze the structure and components of a metastore.
-- MAGIC 2. Implement SQL queries to analyze current catalogs, schemas, tables, and views within a classroom setup.
-- MAGIC 3. Implement row and column security techniques such as column masking and row filtering using SQL functions.
-- MAGIC 4. Develop user-defined functions to perform column masking and row filtering based on specific criteria.
-- MAGIC 5. Design dynamic views to protect columns and rows by applying functions conditional on user identity or group membership.

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
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Let's run the following cell to perform some setup. In order to avoid conflicts in a shared training environment, this will generate a unique catalog name exclusively for your use, which we will use shortly.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-new-02

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 2: Controlling Access to Data
-- MAGIC
-- MAGIC In this section we're going to configure permissions on data objects we created. To keep things simple, we will be granting privileges to everyone. If you're working with a group, you can have others in the group test your work by attempting to access your data objects.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Generate an SQL Query to Access the Table
-- MAGIC The output of the following cell represents a query you could have others in your group run to attempt to access your *gold* table. This simply expands the locally defined variable to provide a complete reference that anyone could execute.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"SELECT * FROM {clean_username}.example.gold")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If someone else were to run this query, this would currently fail since no privileges have been granted yet. Only you (the owner) can access the table at the current time.
-- MAGIC
-- MAGIC By default, no permissions are implied by the metastore. In order to access any data objects, users need appropriate permissions for the data object in question (a view, in this case), as well as all containing elements (the schema and catalog).
-- MAGIC
-- MAGIC Unity catalogs's security model accommodates two distinct patterns for managing data access permissions:
-- MAGIC
-- MAGIC 1. Granting permissions in masses by taking advantage of Unity Catalog's privilege inheritance.
-- MAGIC 1. Explicitly granting permissions to specific objects. This pattern is quite secure, but involves more work to set up and administer.
-- MAGIC
-- MAGIC We'll explore both approaches to provide an understanding of how each one works.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Inherited Privileges
-- MAGIC
-- MAGIC As we've seen, securable objects in Unity Catalog are hierarchical, and privileges are inherited downward. Using this property makes it easy to set up default access rules for your data. Using privilege inheritance, let's build a permission chain that will allow anyone to access the *gold* view.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG {clean_username} TO `account users`")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If someone else were to attempt to run the query from earlier again, the query would succeed because all the appropriate permissions are in place. That is:
-- MAGIC
-- MAGIC * **`USE CATALOG`** on the catalog
-- MAGIC * **`USE SCHEMA`** on the schema
-- MAGIC * **`SELECT`** on the view
-- MAGIC
-- MAGIC All of these permissions were granted at the catalog level with one single statement. As convenient as this is, there are some very important things to keep in mind with this approach:
-- MAGIC
-- MAGIC * The grantee (everyone, in this case) now has the **`SELECT`** privilege on **all** applicable objects (that is, tables and views) in **all** schemas within the catalog
-- MAGIC * This privilege will also be extended to any future tables/views, as well as any future schemas that appear within the catalog
-- MAGIC
-- MAGIC While this can be very convenient for granting access to hundreds or thousands of tables, we must be very careful how we set this up when using privilege inheritance because it's much easier to grant permissions to the wrong things accidentally. Also keep in mind the above approach is extreme. A slightly less permissive compromise can be made, while still leveraging privilege inheritance, with the following two grants. Note, you don't need to run these statements; they're merely provided as an example to illustrate the different types of privilege structures you can create that take advantage of inheritance.
-- MAGIC ```
-- MAGIC GRANT USE CATALOG ON CATALOG ${clean_username} TO `account users`;
-- MAGIC GRANT USE SCHEMA,SELECT ON CATALOG ${clean_username}.example TO `account users`
-- MAGIC ```
-- MAGIC Basically, this pushes the **`USE SCHEMA`** and **`SELECT`** down a level, so that grantees only have access to all applicable objects in the *example* schema.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.3 Revoking Privileges
-- MAGIC
-- MAGIC No data governance platform would be complete without the ability to revoke previously issued grants. In preparation for testing the next approach to granting privileges, let's unwind what we just did using **`REVOKE`**.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"REVOKE USE CATALOG,USE SCHEMA,SELECT ON CATALOG {clean_username} FROM `account users`")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.4 Explicit Privileges
-- MAGIC
-- MAGIC Using explicit privilege grants, let's build a permission chain that will allow anyone to access the *gold* view.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"GRANT USE CATALOG ON CATALOG {clean_username} TO `account users`;")
-- MAGIC spark.sql(f"GRANT USE SCHEMA ON SCHEMA {clean_username}.example TO `account users`;")
-- MAGIC spark.sql(f"GRANT SELECT ON VIEW {clean_username}.example.gold TO `account users`")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC With these grants in place, if anyone else were to query the view again, the query still succeeds because all the appropriate permissions are in place; we've just taken a very different approach to establishing them.
-- MAGIC
-- MAGIC This seems more complicated. One statement from earlier has been replaced with three, and this only provides access to a single view. Following this pattern, we'd have to do an additional **`SELECT`** grant for each additional table or view we wanted to permit. But this complication comes with the benefit of security. Now, users can only read the *gold* view, but nothing else. There's no chance they could accidentally get access to some other object. So this is very explicit and secure, but one can imagine it would be very cumbersome when dealing with lots of tables and views.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views vs. Tables
-- MAGIC
-- MAGIC We've explored two different approaches to managing permissions, and we now have permissions configured such that anyone can access the *gold* view, which processes and displays data from the *silver* table. 
-- MAGIC
-- MAGIC But suppose someone else were to try to directly access the *silver* table. This could be accomplished by replacing *gold* in the previous query with *silver*.
-- MAGIC
-- MAGIC With explicit privileges in place, the query would fail. How then, does the query against the *gold* view work? Because the view's **owner** has appropriate privileges on the *silver* table (through ownership). This property gives rise to interesting applications of views in table security, which we cover in the next section.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 3: Row and Column Security
-- MAGIC Column masks and row filters are techniques used in Databricks to implement fine-grained access control. These methods involve adding additional metadata to tables to specify functions that either mask column values or filter rows based on specific conditions.
-- MAGIC
-- MAGIC To implement column masking, functions are created for each column that needs to be masked. These user-defined functions \(UDFs\) contain the logic to conditionally mask column values.
-- MAGIC
-- MAGIC Row filters, on the other hand, allow you to apply a filter to a table so that only rows meeting certain criteria are returned in subsequent queries.
-- MAGIC
-- MAGIC While column masking requires a separate function for each masked column, row filtering only requires a single function to filter any number of rows.
-- MAGIC
-- MAGIC In both cases, the masking or filtering function is evaluated at query runtime, replacing references to the target column with the results of the function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.1 Column Masking
-- MAGIC Let us implement column masking on the **silver** table and analyze it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.1 Query the Table before Masking
-- MAGIC Let us analyze the **silver** table before applying a column mask.

-- COMMAND ----------

SELECT * FROM silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.2 Create a Function to Perform Column Masking
-- MAGIC Let us recreate a function **mrn_mask** to redact the mrn columns.

-- COMMAND ----------

CREATE FUNCTION mrn_mask(mrn STRING)
  RETURN CASE WHEN is_member('metastore_admins') THEN mrn ELSE 'REDACTED' END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.3 Alter the Table to Apply the Mask
-- MAGIC Let us alter the **silver** table to apply the mask function to redact the mrn column.

-- COMMAND ----------

ALTER TABLE silver ALTER COLUMN mrn SET MASK mrn_mask;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.4 Query the Table with Masking
-- MAGIC Let us analyze the **silver** table after applying the column mask.

-- COMMAND ----------

SELECT * FROM silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.5 Alter the Table to Drop the Mask
-- MAGIC Let us alter the **silver** table to drop the mask function.

-- COMMAND ----------

ALTER TABLE silver ALTER COLUMN mrn DROP MASK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.6 Query the Table after Removing the Mask
-- MAGIC Let us analyze the silver table after removing the column mask.

-- COMMAND ----------

SELECT * FROM silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.1.7 Drop the Mask Function
-- MAGIC Let us drop the **mrn_mask** function that we had created earlier.

-- COMMAND ----------

DROP FUNCTION IF EXISTS mrn_mask;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3.2 Row Filtering
-- MAGIC Let us implement row filtering on the **silver** table and analyze it.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.1 Query the Table before Row Filtering
-- MAGIC Let us analyze the **silver** table before applying a row filter.

-- COMMAND ----------

SELECT * FROM silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.2 Create a Function to Perform Row Filtering
-- MAGIC Let us create a function **device_filter** to filter out rows whose **device_id** is less than 30.

-- COMMAND ----------

CREATE FUNCTION device_filter(device_id INT)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, device_id < 30);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.3 Alter the Table to Apply the Row Filter
-- MAGIC Let us alter the **silver** table to apply the row filter function to filter out rows whose device_id is less than 30.

-- COMMAND ----------

ALTER TABLE silver SET ROW FILTER device_filter ON (device_id);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.4 Query the Table with Row Filtering
-- MAGIC Let us analyze the **silver** table after applying the row filter.

-- COMMAND ----------

SELECT * FROM silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.5 Alter the Table to Drop the Row Filter
-- MAGIC Let us alter the **silver** table to drop the row filter function.

-- COMMAND ----------

ALTER TABLE silver DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.6 Query the Table after Removing the Row Filter
-- MAGIC Let us analyze the **silver** table after removing the row filter.

-- COMMAND ----------

SELECT * FROM silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3.2.7 Drop the Row Filter Function
-- MAGIC Let us drop the **device_filter** function that we had created earlier.

-- COMMAND ----------

DROP FUNCTION IF EXISTS device_filter;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 4: Protecting Columns and Rows with Dynamic Views
-- MAGIC
-- MAGIC Now, let's explore dynamic views, an alternative approach to securing rows and columns. While they have been available in Databricks for some time, they are no longer the primary method for controlling access to rows and columns. However, they still serve a purpose in certain scenarios.
-- MAGIC
-- MAGIC We have seen that Unity Catalog's treatment of views provides the ability for views to protect access to tables; users can be granted access to views that manipulate, transform, or obscure data from a source table, without needing to provide direct access to the source table.
-- MAGIC
-- MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do things like:
-- MAGIC * partially obscure column values or redact them entirely
-- MAGIC * omit rows based on specific criteria
-- MAGIC
-- MAGIC Access control with dynamic views is achieved through the use of functions within the definition of the view. These functions include:
-- MAGIC * **`current_user()`**: returns the email address of the user querying the view
-- MAGIC * **`is_account_group_member()`**: returns TRUE if the user querying the view is a member of the specified group
-- MAGIC * **`is_member()`**: returns TRUE if the user querying the view is a member of the specified workspace-local group
-- MAGIC
-- MAGIC Note: Databricks generally advises against using the **`is_member()`** function in production, since it references workspace-local groups and hence introduces a workspace dependency into a metastore that potentially spans multiple workspaces.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.1 Redacting columns
-- MAGIC
-- MAGIC Suppose we want everyone to be able to see aggregated data trends from the *gold* view, but we don't want to disclose patient PII to everyone. Let's redefine the view to redact the *mrn* and *name* columns, so that only members of *metastore_admins* can see it, using the **`is_account_group_member()`** function.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.1.1 Recreate the View
-- MAGIC Let us recreate a **gold** view while redacting the mrn and name columns.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('metastore_admins') THEN mrn 
    ELSE 'REDACTED'
  END AS mrn,
  CASE WHEN
    is_account_group_member('metastore_admins') THEN name
    ELSE 'REDACTED'
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver
  GROUP BY mrn, name, DATE_TRUNC("DD", time);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.1.2 Re-issue Grant Access to View
-- MAGIC We'll re-issue the grant since the above statement replaced the previous object and thus any grants applied directly on the object would have been lost.

-- COMMAND ----------

GRANT SELECT ON VIEW gold TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.1.3 Query the View
-- MAGIC Now let's query the view.

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Does this output surprise you?
-- MAGIC
-- MAGIC As the owner of the view and table, you do not need any privileges to access these objects, yet when querying the view, we see redacted columns. This is because of the way the view is defined. As a regular user (one who is not a member of the **`metastore_admins`** group), the *mrn* and *name* columns are redacted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.2 Restrict Rows
-- MAGIC
-- MAGIC Now let's suppose we want a view that, rather than aggregating and redacting columns, simply filters out rows from the source. Let's  apply the same **`is_account_group_member()`** function to create a view that passes through only rows whose *device_id* is less than 30. Row filtering is done by applying the conditional as a **`WHERE`** clause.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.2.1 Recreate the View
-- MAGIC Let us recreate a **gold** view while filtering out the rows from the source.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('metastore_admins') THEN TRUE
    ELSE device_id < 30
  END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.2.2 Re-issue Grant Access to View
-- MAGIC We'll re-issue the grant since the above statement replaced the previous object and thus any grants applied directly on the object would have been lost.

-- COMMAND ----------

-- Re-issue the grant --
GRANT SELECT ON VIEW gold TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.2.3 Query the View
-- MAGIC Now let's query the view.

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Three records are omitted. Those records contained values for *device_id* that were caught by the filter. Only members of **`metastore_admins`** would see those.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.3 Data Masking
-- MAGIC One final use case for dynamic views is data masking, or partially obscuring data. This is fairly common practice (for example, displaying the last 4 digits of a credit card number, or the last two digits of a phone number). Masking is similar in principle to redaction except we are displaying some of the data rather than displaying none of it. And for this simple example, we'll leverage the *dbacademy_mask()* user-defined function that we created earlier to mask the *mrn* column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.3.1 Recreate the View
-- MAGIC Let us recreate a **gold** view and apply data masking on the mrn column. We'll also re-issue the grant since the below statement will replace the previous object and thus any grants applied previously on the object will be lost.

-- COMMAND ----------

CREATE OR REPLACE VIEW gold AS
SELECT
  CASE WHEN
    is_account_group_member('metastore_admins') THEN mrn
    ELSE dbacademy_mask(mrn)
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver
WHERE
  CASE WHEN
    is_account_group_member('metastore_admins') THEN TRUE
    ELSE device_id < 30
  END;

-- Re-issue the grant --
GRANT SELECT ON VIEW gold TO `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4.3.2 Query the View
-- MAGIC Now let's query the view.

-- COMMAND ----------

SELECT * FROM gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For us, all values in the *mrn* column will be masked.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC In this demo, we delved into the functionalities of Databricks' metastore, emphasizing fine-grained access control through techniques such as column masking, row filtering, and dynamic views. By analyzing the structure and components of the metastore, implementing SQL queries, and managing permissions on data objects, we gained a comprehensive understanding of metadata management and security measures. Through practical exercises, we learned to implement row and column security techniques, including creating user-defined functions for masking and filtering, and designing dynamic views for conditional access control based on user identity or group membership. This hands-on exploration equipped us with essential skills to effectively manage metadata and enforce security policies to safeguard sensitive data within a Databricks environment.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
