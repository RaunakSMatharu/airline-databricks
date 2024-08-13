-- Databricks notebook source
use publish_airline

-- COMMAND ----------

create table if not exists dim_plane(
  tailid STRING,
  type STRING,
  manufacturer STRING,
  issue_date date,
  model STRING,
  status STRING,
  aircraft_type STRING,
  engine_type STRING,
  year INT
) USING delta

location '/mnt/publishstoragegen2acc/Dim_Plane/'

-- COMMAND ----------

Insert overwrite Dim_Plane 
select 
 tailid,
  type,
  manufacturer,
  issue_date,
  model,
  status,
  aircraft_type,
  engine_type,
  year from cleansed_airline.plane 

-- COMMAND ----------


