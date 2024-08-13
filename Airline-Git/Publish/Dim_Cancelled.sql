-- Databricks notebook source
use publish_airline;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Dim_Cancellation (
  code STRING,
  description STRING
) USING DELTA LOCATION '/mnt/publishstoragegen2acc/Dim_Cancellation'


-- COMMAND ----------

INSERT OVERWRITE Dim_Cancellation
SELECT 
code 
,description 
FROM  cleansed_airline.cancellation

-- COMMAND ----------


