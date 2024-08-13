-- Databricks notebook source
use publish_airline;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airport (
  airport_name STRING,
  code STRING,
  city STRING,
  country STRING
) USING DELTA LOCATION '/mnt/publishstoragegen2acc/Dim_Airport'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airport
SELECT 
airport_name,
code,city,country
FROM  cleansed_airline.airport 

-- COMMAND ----------


