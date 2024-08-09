# Databricks notebook source
import tabula
from datetime import date
import pandas as pd
print(date.today())

# COMMAND ----------

list_file=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/sourceblob/') if(i.name.split('.')[1]=='pdf')]
print(list_file)

# COMMAND ----------

def f_source_pdf_datalake(source_path,sink_path,output_format,page,file_name):
    try:
        dbutils.fs.mkdirs(f"/{sink_path}{file_name.split('.')[0]}/Date_Part={date.today()}/")
        tabula.convert_into(f'{source_path}{file_name}',f"/dbfs/{sink_path}/{file_name.split('.')[0]}/Date_Part={date.today()}/{file_name.split('.')[0]}.{output_format}",output_format=output_format,pages=page)
    except Exception as err:
        print("error occured",str(err))

# COMMAND ----------

list_file=[(i.name,i.name.split('.')[1]) for i in dbutils.fs.ls('/mnt/sourceblob/') if(i.name.split('.')[1]=='pdf')]
for i in list_file:
    f_source_pdf_datalake('/dbfs/mnt/sourceblob/','mnt/sinkstoragegen2acc/','csv','all',i[0])
