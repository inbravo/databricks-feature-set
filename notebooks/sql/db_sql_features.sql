-- Databricks notebook source
/* 
  A database is a collection of data objects, such as "TABLES", "VIEWS", and "FUNCTIONS". 
  In Databricks, the terms SCHEMA and “DATABASE” are used interchangeably.
  While usage of SCHEMA and DATABASE is interchangeable, SCHEMA is preferred: "CREATE SCHEMA"
*/
/* Review the current Databases */
SHOW DATABASES;
/* Create Databases for medallion architecture based data lake */
CREATE DATABASE IF NOT EXISTS lms_bronze LOCATION 'dbfs:/user/hive/warehouse/lms_bronze.db';
CREATE DATABASE IF NOT EXISTS lms_silver LOCATION 'dbfs:/user/hive/warehouse/lms_silver.db';
CREATE DATABASE IF NOT EXISTS lms_gold LOCATION 'dbfs:/user/hive/warehouse/lms_gold.db';
/* Review the newly created Databases/Schemas */
DESCRIBE DATABASE lms_silver;
DESCRIBE DATABASE lms_bronze;
DESCRIBE DATABASE lms_gold;
/* 
  To delete a database use following command: "DROP DATABASE"
  While usage of SCHEMA and DATABASE is interchangeable, SCHEMA is preferred
*/
-- DROP DATABASE IF EXISTS lms_bronze CASCADE;
-- DROP DATABASE IF EXISTS lms_silver CASCADE;
-- DROP DATABASE IF EXISTS lms_gold CASCADE;

-- COMMAND ----------

/* Select few columns from Silver a Zone table */
SELECT COUNTRY, PRICE FROM LMS_SILVER.BOOKS;

-- COMMAND ----------

/* Get the record count from a Silver Zone table */
SELECT COUNT(*) FROM LMS_SILVER.BOOKS;

-- COMMAND ----------

/* Get the desciption of a Silver Zone table */
DESCRIBE FORMATTED LMS_SILVER.BOOKS;

-- COMMAND ----------

/* Select top 10 records from a Silver Zone table */
SELECT * FROM LMS_SILVER.BOOKS LIMIT 10;

-- COMMAND ----------

/* Select the count of a column from a Silver Zone table */
SELECT COUNT(COUNTRY) FROM LMS_SILVER.BOOKS;
-- SELECT COUNT(DISTINCT(COUNTRY)) FROM LMS_SILVER.BOOKS;
-- SELECT COUNT(DISTINCT(TITLE)) FROM LMS_SILVER.BOOKS;

-- COMMAND ----------

/* "COUNT_IF" to count the records for a certain column value */
SELECT COUNT_IF(country = 'France') FROM LMS_SILVER.BOOKS;

-- COMMAND ----------

/* "FROM_UNIXTIME" converts the given timestamp to the timestamp in the desired format */
SELECT FROM_UNIXTIME(PUBLISH_TIME, 'yyyy-MM-dd HH:mm') FROM LMS_SILVER.BOOKS;
/* "CAST" converts the given timestamp other desired format */
SELECT CAST(CAST(PUBLISH_TIME AS BIGINT) AS TIMESTAMP) FROM LMS_SILVER.BOOKS;
/* "YEAR" helps to get the year details out of stored timestamp */
SELECT * FROM LMS_SILVER.BOOKS WHERE YEAR(FROM_UNIXTIME(PUBLISH_TIME)) > 1900;

-- COMMAND ----------

/* Use of LIKE keyword */
SELECT * FROM LMS_SILVER.BOOKS WHERE TITLE LIKE '%E%'

-- COMMAND ----------

/* Select the total price from two countries of a Silver Zone table */
SELECT * FROM (
  SELECT COUNTRY, PRICE FROM LMS_SILVER.BOOKS
) PIVOT (
  SUM(PRICE) FOR COUNTRY IN ('FRANCE','GERMANY')
);

-- COMMAND ----------

/* Create a User Defined Function (UDF) */
CREATE OR REPLACE FUNCTION CONVERT_F_TO_C(TEMP DOUBLE)
RETURNS DOUBLE

RETURN (TEMP - 32) * (5/9);

-- COMMAND ----------

/* 
  Use a User Defined Function (UDF)
*/
SELECT CONVERT_F_TO_C(100);

-- COMMAND ----------

/* 
  Get the details of User Defined Function (UDF)
*/
DESCRIBE FUNCTION EXTENDED CONVERT_F_TO_C;

-- COMMAND ----------

/* 
  Use of case statement on table data
*/
SELECT *, CASE 
   WHEN PRICE > 15 THEN 'EXPENSIVE' 
   ELSE 'CHEAP' 
   END
FROM LMS_SILVER.BOOKS;

-- COMMAND ----------

/* 
  Use of case statement on table data
*/
SELECT * FROM LMS_SILVER.BOOKS ORDER BY (CASE 
   WHEN PAGES < 500 
   THEN PAGES 
   ELSE PRICE 
END);

-- COMMAND ----------

/* 
  View is a virtual table that has no physical data based on the result-set of a SQL query
  "ALTER VIEW" and "DROP VIEW" only change metadata.
*/
CREATE VIEW IF NOT EXISTS VIEW_SPAIN AS SELECT * FROM LMS_SILVER.BOOKS WHERE COUNTRY = 'SPAIN';
/*  
  A Temp View created in one notebook isn't accessible to others 
  TEMP views are visible only to the session that created them and are dropped when the session ends.
  TEMP views are not materialized views. Read more here- https://community.databricks.com/t5/data-engineering/how-do-temp-views-actually-work/td-p/20136
*/
CREATE TEMP VIEW IF NOT EXISTS TEMP_SPAIN AS SELECT * FROM LMS_SILVER.BOOKS WHERE COUNTRY = 'SPAIN';
/* 
  If you need to share view across notebooks, use Global Temp View instead 
  GLOBAL TEMP views are tied to a system preserved temporary schema global_temp.
*/
CREATE GLOBAL TEMP VIEW  IF NOT EXISTS VIEW_SPAIN AS SELECT * FROM LMS_SILVER.BOOKS WHERE COUNTRY = 'SPAIN';

-- COMMAND ----------

SELECT * FROM GLOBAL_TEMP.VIEW_SPAIN;
