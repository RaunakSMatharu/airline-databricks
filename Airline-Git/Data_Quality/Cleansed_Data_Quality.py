# Databricks notebook source
# MAGIC %run "/Workspace/Airline-Project/utilities/Common Functions"

# COMMAND ----------


list_table_info=[('STREAMING UPDATE','airline',100),
                 ('STREAMING UPDATE','airport',200),
                 ('STREAMING UPDATE','cancellation',100),
                 ('STREAMING UPDATE','flights',400),
                 ('STREAMING UPDATE','plane',200),
                 ('STREAMING UPDATE','unique_carriers',1200)]

for i in list_table_info:
    count_check('cleansed_airline',i[0],i[1],i[2])

# COMMAND ----------


