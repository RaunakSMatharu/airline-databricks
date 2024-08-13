# Databricks notebook source
dbutils.widgets.text("Layer_name","")
Layer_name=dbutils.widgets.getArgument("Layer_name")

# COMMAND ----------

NoteBook_path_Json = {
    "raw": ["Airline-Git/raw_sourcing/raw_souring"],
    "cleansed": [
        "Airline-Git/Cleansed/Cleansing_airlines",
        "Airline-Git/Cleansed/Cleansing_airport",
        "Airline-Git/Cleansed/Cleansing_cancellation",
        "Airline-Git/Cleansed/Cleansing_flight",
        "Airline-Git/Cleansed/Cleansing_Plane",
        "Airline-Git/Cleansed/Cleansing_Unique_carrier"
    ],
    "Data_Check":["Airline-Git/Data_Quality/Cleansed_Data_Quality"],
    "publish": [
        "Airline-Git/Publish/Dim_airline",
        "Airline-Git/Publish/Dim_Airport",
        "Airline-Git/Publish/Dim_Cancelled",
        "Airline-Git/Publish/Dim_Plane",
        "Airline-Git/Publish/Dim_Unique_Carriers",
        "Airline-Git/Publish/Reporting_Flight"
    ]
}

# COMMAND ----------

for notebook_path in NoteBook_path_Json[Layer_name]:
    print("/Workspace/Repos/mihirpatkar28@gmail.com/airline/"+notebook_path)
    dbutils.notebook.run("/Workspace/Repos/mihirpatkar28@gmail.com/airline/"+notebook_path,0)
