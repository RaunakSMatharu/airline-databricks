# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format",'csv')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/PLANE")\
        .load('/mnt/sinkstoragegen2acc/PLANE/')

# COMMAND ----------

df_base=df.selectExpr("tailnum as tailid","type","manufacturer","to_date(issue_date) as issue_date","model","status","aircraft_type","engine_type","cast('year' as int) as year","to_date(Date_Part) as Date_Part")

df_base.writeStream.trigger(once=True).\
    format("delta").\
        option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/PLANE")\
        .start("/mnt/cleanstoragegen2acc/plane")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleanstoragegen2acc/plane')
schema=pre_schema(df)
f_delta_cleansed_load('plane','/mnt/cleanstoragegen2acc/plane','cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cleansed_airline.plane
