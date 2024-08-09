# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.json("/mnt/sinkstoragegen2acc/airlines/")

# COMMAND ----------

df_base1=df.select('response','Date_Part')
df_base=df_base1.select(explode("response").alias('response'),'Date_Part')
df_final=df_base.select('response.iata_code','response.icao_code','response.name',"Date_Part")

# COMMAND ----------

df_final.write.format('delta').mode("overwrite").save("/mnt/cleanstoragegen2acc/airline/")

# COMMAND ----------

# Example usage
f_delta_cleansed_load('airline', '/mnt/cleanstoragegen2acc/airline', 'cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_airline.airline

# COMMAND ----------


