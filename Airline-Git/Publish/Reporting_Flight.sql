-- Databricks notebook source
use publish_airline

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('/mnt/publishstoragegen2acc/Reporting_Flight',True)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Reporting_Flight (
  date date,
  date_year int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Cancelled string,
  CancellationCode string,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  deptime string
)
USING DELTA 
PARTITIONED BY (date_year) 
LOCATION '/mnt/publishstoragegen2acc/Reporting_Flight'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #max_year=spark.sql("select year(max(date)) from cleansed_geekcoders.flight").collect()[0][0]
-- MAGIC max_year=2005

-- COMMAND ----------

INSERT OVERWRITE TABLE Reporting_Flight PARTITION (date_year)
SELECT
  date,
  year(date) as date_year,
  ArrDelay,
  DepDelay,
  Origin,
  Cancelled,
  CancellationCode,
  UniqueCarrier,
  FlightNum,
  TailNum,
  deptime
FROM
  cleansed_airline.flights
