-- Databricks notebook source
use publish_airline;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Airlines (
  iata_code STRING,
  icao_code STRING,
  name STRING
) USING DELTA LOCATION '/mnt/publishstoragegen2acc/Dim_Airlines'

-- COMMAND ----------

INSERT OVERWRITE Dim_Airlines
SELECT 
iata_code 
,icao_code 
,name 
FROM  cleansed_airline.airline

-- COMMAND ----------


