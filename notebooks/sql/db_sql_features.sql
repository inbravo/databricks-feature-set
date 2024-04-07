-- Databricks notebook source
SHOW DATABASES;

CREATE DATABASE IF NOT EXISTS lms_bronze LOCATION 'dbfs:/user/hive/warehouse/lms_bronze.db';
CREATE DATABASE IF NOT EXISTS lms_silver LOCATION 'dbfs:/user/hive/warehouse/lms_silver.db';
CREATE DATABASE IF NOT EXISTS lms_gold LOCATION 'dbfs:/user/hive/warehouse/lms_gold.db';

DESCRIBE DATABASE lms_silver;
DESCRIBE DATABASE lms_bronze;
DESCRIBE DATABASE lms_gold;

DROP DATABASE IF EXISTS lms_bronze CASCADE;
DROP DATABASE IF EXISTS lms_silver CASCADE;
DROP DATABASE IF EXISTS lms_gold CASCADE;

-- COMMAND ----------

SELECT country, price FROM lms_silver.books;

-- COMMAND ----------

SELECT count(*) FROM lms_silver.books;

-- COMMAND ----------



-- COMMAND ----------

DESCRIBE FORMATTED lms_silver.books;

-- COMMAND ----------

SELECT * FROM lms_silver.books LIMIT 10;

-- COMMAND ----------

SELECT count(country) FROM lms_silver.books;
-- SELECT count(distinct(country)) FROM lms_silver.books;
-- SELECT count(distinct(title)) FROM lms_silver.books;

-- COMMAND ----------

SELECT * FROM (
  SELECT country, price FROM lms_silver.books
) PIVOT (
  sum(price) for country in ('France','Germany')
);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION convert_f_to_c(temp DOUBLE)
RETURNS DOUBLE

RETURN (temp - 32) * (5/9);

-- COMMAND ----------

SELECT convert_f_to_c(100);

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED convert_f_to_c;

-- COMMAND ----------

SELECT *, CASE 
   WHEN price > 15 THEN 'expensive' 
   ELSE 'cheap' 
   END
FROM dbacademy.books;

-- COMMAND ----------

SELECT * FROM lms_silver.books ORDER BY (CASE 
   WHEN pages < 500 
   THEN pages 
   ELSE price 
END);

