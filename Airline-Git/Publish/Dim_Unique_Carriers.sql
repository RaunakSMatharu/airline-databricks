-- Databricks notebook source
use publish_airline;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_UniqueCarrier (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/publishstoragegen2acc/Dim_UniqueCarrier'

-- COMMAND ----------

INSERT OVERWRITE Dim_UniqueCarrier
SELECT 
code 
,description 
FROM  cleansed_airline.unique_carriers
