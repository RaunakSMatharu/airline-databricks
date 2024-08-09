# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format",'parquet')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/Cancellation")\
        .load('/mnt/sinkstoragegen2acc/Cancellation/')

# COMMAND ----------

from pyspark.sql.functions import *

df_base = df.selectExpr(
     "replace(Code,'\"','') as code",
    "replace(Description,'\"','') as description",
    "date_format(Date_Part,'yyyy-MM-dd') as Date_Part"
)

# COMMAND ----------

df_base.writeStream.trigger(once=True).\
    format("delta").\
        option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/Cancellation")\
        .start("/mnt/cleanstoragegen2acc/cancellation")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleanstoragegen2acc/cancellation')
schema=pre_schema(df)

# COMMAND ----------

f_delta_cleansed_load('cancellation','/mnt/cleanstoragegen2acc/cancellation','cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed_airline.cancellation

# COMMAND ----------


