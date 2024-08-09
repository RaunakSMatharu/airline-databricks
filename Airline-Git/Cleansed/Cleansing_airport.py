# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format",'csv')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/airport")\
        .load('/mnt/sinkstoragegen2acc/Airport/')

# COMMAND ----------

from pyspark.sql.functions import *

df_base = df.selectExpr(
    "Code as code",
    "split(Description, ',')[0] as city",
    "split(split(Description, ',')[1],':')[0] as country",
    "split(split(Description, ',')[1],':')[1] as airport_name"
).withColumn("Date_Part", current_date())


df_base.writeStream.trigger(once=True).\
    format("delta").\
        option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/airport")\
        .start("/mnt/cleanstoragegen2acc/airport")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleanstoragegen2acc/airport')
schema=pre_schema(df)
schema

# COMMAND ----------

f_delta_cleansed_load('airport','/mnt/cleanstoragegen2acc/airport','cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed_airline.airport

# COMMAND ----------


