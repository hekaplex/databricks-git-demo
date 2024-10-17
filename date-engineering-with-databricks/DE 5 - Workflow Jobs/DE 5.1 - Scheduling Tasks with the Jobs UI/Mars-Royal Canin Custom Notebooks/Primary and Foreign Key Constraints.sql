-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databricks Primary and Foreign Keys
-- MAGIC
-- MAGIC ##### Objectives
-- MAGIC 1. Define primary and foriegn key constraints
-- MAGIC 1. Display constraints
-- MAGIC 1. Use the `RELY` option to enable optimizations
-- MAGIC 1. Speed up your queries by eliminating unnecessary aggregations
-- MAGIC 1. Speed up your queries by eliminating unnecessary joins
-- MAGIC
-- MAGIC ##### Primary and Foreign Key Constraints References in Unity Catalog
-- MAGIC - <a data-external-link="true" href="https://docs.databricks.com/en/sql/language-manual/sql-ref-information-schema.html" target="_blank" rel="noopener noreferrer">INFORMATION_SCHEMA</a>
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/information-schema/table_constraints.html" target="_blank"><code>TABLE_CONSTRAINTS</code> </a>: Describes metadata for all primary and foreign key constraints within the catalog.</a>
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/information-schema/key_column_usage.html" target="_blank"><code>KEY_COLUMN_USAGE</code></a>: Lists the columns of the primary or foreign key constraints within the catalog.</a>
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/information-schema/constraint_table_usage.html" target="_blank"><code>CONSTRAINT_TABLE_USAGE</code></a>: Describes the constraints referencing tables in the catalog.</a>
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/information-schema/constraint_column_usage.html" target="_blank"><code>CONSTRAINT_COLUMN_USAGE</code></a>: Describes the constraints referencing columns in the catalog.</a>
-- MAGIC - <a href="https://docs.databricks.com/en/sql/language-manual/information-schema/referential_constraints.html" target="_blank"><code>REFERENTIAL_CONSTRAINTS</code></a>: Describes referential (foreign key) constraints defined in the catalog.</a>
-- MAGIC
-- MAGIC ## Introduction
-- MAGIC Primary Keys (PKs) and Foreign Keys (FKs) are essential elements in relational databases, acting as fundamental building blocks for data modeling. They provide information about the data relationships in the schema to users, tools and applications; and enable optimizations that leverage constraints to speed up queries. Primary and foreign keys are now generally available for your Delta Lake tables hosted in Unity Catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Language
-- MAGIC You can define constraints when you create a table:

-- COMMAND ----------

USE CATALOG mars_royal_canin;
CREATE SCHEMA IF NOT EXISTS pk_fk_example;
USE SCHEMA pk_fk_example;

CREATE TABLE Users (
    UserID INT NOT NULL PRIMARY KEY,
    UserName STRING,
    Email STRING,
    SignUpDate DATE
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the above example, we define a primary key constraint on the column UserID. Databricks also supports constraints on groups of columns as well.
-- MAGIC
-- MAGIC You can also modify existing Delta tables to add or remove constraints:

-- COMMAND ----------

CREATE OR REPLACE TABLE Products (
    ProductID INT NOT NULL,
    ProductName STRING,
    Price DECIMAL(10,2),
    CategoryID INT
);
ALTER TABLE Products ADD CONSTRAINT products_pk PRIMARY KEY (ProductID);
--ALTER TABLE Products DROP CONSTRAINT products_pk;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here we create the primary key named products_pk on the non-nullable column `ProductID` in an existing table. To successfully execute this operation, you must be the owner of the table. Note that constraint names must be unique within the schema.
-- MAGIC The subsequent command removes the primary key by specifying the name.
-- MAGIC
-- MAGIC The same process applies for foreign keys. The following table defines two foreign keys at table creation time:

-- COMMAND ----------

CREATE TABLE Purchases (
    PurchaseID INT PRIMARY KEY,
    UserID INT,
    ProductID INT,
    PurchaseDate DATE,
    Quantity INT,
    FOREIGN KEY (UserID) REFERENCES Users(UserID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Please refer to the documentation on [CREATE TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html) and [ALTER TABLE](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-alter-table.html) statements for more details on the syntax and operations related to constraints.
-- MAGIC
-- MAGIC Primary key and foreign key constraints aren't enforced in the Databricks engine, but they may be useful for indicating a data integrity relationship that is intended to hold true. Databricks can instead enforce primary key constraints upstream as part of the ingest pipeline. See [Managed data quality with Delta Live Tables ](https://docs.databricks.com/en/delta-live-tables/expectations.html) for more information on enforced constraints. Databricks also supports enforced NOT NULL and CHECK constraints (see the [Constraints documentation](https://docs.databricks.com/en/tables/constraints.html) for more information).
-- MAGIC
-- MAGIC Partner Ecosystem
-- MAGIC Tools and applications such as the latest version of Tableau and PowerBI can automatically import and utilize your primary key and foreign key relationships from Databricks through JDBC and ODBC connectors.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View the constraints
-- MAGIC There are several ways to view the primary key and foreign key constraints defined in the table. You can also simply use SQL commands to view constraint information with the `DESCRIBE TABLE EXTENDED` command:

-- COMMAND ----------

DESCRIBE TABLE EXTENDED Products

-- COMMAND ----------

DESCRIBE TABLE EXTENDED Purchases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Catalog Explorer and Entity Relationship Diagram
-- MAGIC You can also view the constraints information through the [Catalog Explorer](https://docs.databricks.com/en/catalog-explorer/index.html):
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://www.databricks.com/sites/default/files/inline-images/db-960-blog-img-1_0.png?v=1720429389" alt="Databricks Learning">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Each primary key and foreign key column has a small key icon next to its name.
-- MAGIC
-- MAGIC And you can visualize the primary and foreign key information and the relationships between tables with the [Entity Relationship Diagram](https://docs.databricks.com/en/catalog-explorer/entity-relationship-diagram.html) in Catalog Explorer. Below is an example of a table purchases referencing two tables, users and products:
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://www.databricks.com/sites/default/files/inline-images/db-960-blog-img-2.png?v=1720429389" alt="Databricks Learning">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use the RELY option to enable optimizations
-- MAGIC If you know that the primary key constraint is valid, (for example, because your data pipeline or ETL job enforces it) then you can enable optimizations based on the constraint by specifying it with the RELY option, like:
-- MAGIC
-- MAGIC `PRIMARY KEY (c_customer_sk) RELY`
-- MAGIC
-- MAGIC Using the RELY option lets Databricks optimize queries in ways that depend on the constraint's validity, because you are guaranteeing that the data integrity is maintained. Exercise caution here because if a constraint is marked as RELY but the data violates the constraint, your queries may return incorrect results.
-- MAGIC
-- MAGIC When you do not specify the RELY option for a constraint, the default is NORELY, in which case constraints may still be used for informational or statistical purposes, but queries will not rely on them to run correctly.
-- MAGIC
-- MAGIC The RELY option and the optimizations utilizing it are currently available for primary keys, and will also be coming soon for foreign keys.
-- MAGIC
-- MAGIC You can modify a table's primary key to change whether it is RELY or NORELY by using ALTER TABLE, for example:

-- COMMAND ----------

ALTER TABLE Users DROP KEY CASCADE;
ALTER TABLE Users ADD PRIMARY KEY (UserID) RELY;

-- COMMAND ----------

ALTER TABLE Purchases ADD CONSTRAINT fk_UserID FOREIGN KEY (UserID) REFERENCES Users(UserID)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Speed up your queries by eliminating unnecessary aggregations
-- MAGIC One simple optimization we can do with RELY primary key constraints is eliminating unnecessary aggregates. For example, in a query that is applying a distinct operation over a table with a primary key using RELY:

-- COMMAND ----------

SELECT DISTINCT UserID FROM Users;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can remove the unnecessary DISTINCT operation:

-- COMMAND ----------

SELECT  UserID FROM Users;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see, this query relies on the validity of the RELY primary key constraint - if there are duplicate customer IDs in the customer table, then the transformed query will return incorrect duplicate results. You are responsible for enforcing the validity of the constraint if you set the RELY option.
-- MAGIC
-- MAGIC If the primary key is NORELY (the default), then the optimizer will not remove the DISTINCT operation from the query. Then it may run slower but always returns correct results even if there are duplicates. If the primary key is RELY, Databricks can remove the DISTINCT operation, which can greatly speed up the query - by about 2x for the above example.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Speed up your queries by eliminating unnecessary joins
-- MAGIC
-- MAGIC Another very useful optimization we can perform with RELY primary keys is eliminating unnecessary joins. If a query joins a table that is not referenced anywhere except in the join condition, then the optimizer can determine that the join is unnecessary, and remove the join from the query plan.
-- MAGIC
-- MAGIC To give an example, let's say we have a query joining two tables, store_sales and customer, joined on the primary key of the customer table `PRIMARY KEY (c_customer_sk) RELY`.

-- COMMAND ----------

SELECT SUM(ss_quantity)
FROM Purchases P
LEFT JOIN Users U
ON P.UserID = U.UserID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC If we didn't have the primary key, each row of `store_sales` could potentially match multiple rows in `customer`, and we would need to execute the join to compute the correct SUM value. But because the table `customer` is joined on its primary key, we know that the join will output one row for each row of `store_sales`.
-- MAGIC
-- MAGIC So the query only actually needs the column `ss_quantity` from the fact table `store_sales`. Therefore, the query optimizer can entirely eliminate the join from the query, transforming it into:

-- COMMAND ----------

SELECT SUM(ss_quantity)
FROM Purchases P

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This runs much faster by avoiding the entire join - in this example we observe the optimization speed up the query from 1.5 minutes to 6 seconds!. And the benefits can be even larger when the join involves many tables that can be eliminated!
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://www.databricks.com/sites/default/files/inline-images/db-960-blog-img-3.png?v=1720429389" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC
-- MAGIC You may ask, why would anyone run a query like this? It's actually much more common than you might think! One common reason is that users construct views that join together several tables, such as joining together many fact and dimension tables. They write queries over these views which often use columns from only some of the tables, not all - and so the optimizer can eliminate the joins against the tables that aren't needed in each query. This pattern is also common in many Business Intelligence (BI) tools, which often generate queries joining many tables in a schema even when a query only uses columns from some of the tables.
-- MAGIC
-- MAGIC ## Conclusion
-- MAGIC Since its public preview, over 2600 + Databricks customers have used primary key and foreign key constraints. Today, we are excited to announce the general availability of this feature, marking a new stage in our commitment to enhancing data management and integrity in Databricks.
-- MAGIC
-- MAGIC Furthermore, Databricks now takes advantage of key constraints with the RELY option to optimize queries, such as by eliminating unnecessary aggregates and joins, resulting in much faster query performance.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
-- MAGIC
-- MAGIC Adapted from [blog article](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
