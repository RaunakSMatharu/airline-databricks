# Databricks notebook source
# MAGIC %run "/Workspace/Repos/mihirpatkar28@gmail.com/airline/Airline-Git/utilities/Common Functions"

# COMMAND ----------

# MAGIC %py
# MAGIC insert_query="select count(*) from publish_airline.dim_uniquecarrier group by code having count(*)>1"
# MAGIC insert_test_cases("publish_airline",1,"Check if code is duplicated in the dim_uniquecarrier or not ",insert_query,0)

# COMMAND ----------

# MAGIC %py
# MAGIC insert_query="select count(*) from publish_airline.dim_airport group by code having count(*)>1"
# MAGIC insert_test_cases("publish_airline",2,"Check if code is duplicated in the dim_airport or not ",insert_query,0)
# MAGIC

# COMMAND ----------


