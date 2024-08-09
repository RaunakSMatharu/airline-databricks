# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------

dbutils.fs.ls('/mnt/sinkstoragegen2acc/flights')

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudfiles.format",'csv')\
    .option("cloudFiles.schemaLocation","/dbfs/FileStore/tables/schema/flights")\
        .load('/mnt/sinkstoragegen2acc/flights/')

# COMMAND ----------

from pyspark.sql.functions import *

from pyspark.sql.functions import concat_ws
# Set the legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Continue with your DataFrame transformations
df_base = df.selectExpr(
    "to_date(concat_ws('-',year,month,dayofmonth),'yyyy-MM-dd') as date",
    "from_unixtime(unix_timestamp(case when DepTime=2400 then '0000' else DepTime End,'HHmm'),'HH:mm')  as deptime",
    "from_unixtime(unix_timestamp(case when CRSDepTime=2400 then '0000' else CRSDepTime End,'HHmm'),'HH:mm')  as CRSDepTime",
    "from_unixtime(unix_timestamp(case when ArrTime=2400 then '0000' else ArrTime End,'HHmm'),'HH:mm')  as ArrTime",
    "from_unixtime(unix_timestamp(case when CRSArrTime=2400 then '0000' else CRSArrTime End,'HHmm'),'HH:mm')  as CRSArrTime",
    "UniqueCarrier",
    "cast(FlightNum as int) as FlightNum",
    "cast(TailNum as int) as TailNum",
    "cast(ActualElapsedTime as int) as ActualElapsedTime",
    "cast(CRSElapsedTime as int) as CRSElapsedTime",
    "cast(AirTime as int) as AirTime",
    "cast(ArrDelay as int) as ArrDelay",
    "cast(DepDelay as int) as DepDelay",
    "Origin",
    "Dest",
    "cast(Distance as int) as Distance",
    "cast(TaxiIn as int) as TaxiIn",
    "cast(TaxiOut as int) as TaxiOut",
    "Cancelled",
    "CancellationCode",
    "cast(Diverted as int) as castDiverted",
    "cast(CarrierDelay as int) as CarrierDelay",
    "cast(WeatherDelay as int) as WeatherDelay",
    "cast(NASDelay as int) as NASDelay",
    "cast(SecurityDelay as int) as SecurityDelay",
    "cast(LateAircraftDelay as int) as LateAircraftDelay",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"
)

# COMMAND ----------

df_base.writeStream.trigger(once=True).\
    format("delta").\
        option("checkpointLocation","/dbfs/FileStore/tables/checkpointLocation/flights")\
        .start("/mnt/cleanstoragegen2acc/flights")

# COMMAND ----------

df=spark.read.format('delta').load('/mnt/cleanstoragegen2acc/flights')
schema=pre_schema(df)

# COMMAND ----------

df.count()

# COMMAND ----------

f_delta_cleansed_load('flights','/mnt/cleanstoragegen2acc/flights','cleansed_airline')

# COMMAND ----------

# MAGIC %sql
# MAGIC select deptime,count(*) from cleansed_airline.flights
# MAGIC group by 1

# COMMAND ----------


