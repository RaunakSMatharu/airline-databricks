# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format",'parquet')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/unique_carriers")\
        .load('/mnt/sinkstoragegen2acc/UNIQUE_CARRIERS/')

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
        option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/unique_carriers")\
        .start("/mnt/cleanstoragegen2acc/unique_carriers")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleanstoragegen2acc/unique_carriers')
schema=pre_schema(df)

# COMMAND ----------

f_delta_cleansed_load('unique_carriers','/mnt/cleanstoragegen2acc/unique_carriers','cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed_airline.unique_carriers

# COMMAND ----------


