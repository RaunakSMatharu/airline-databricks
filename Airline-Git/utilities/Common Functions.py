# Databricks notebook source
# def pre_schema(location):
#     try:
#         df=spark.read.format('delta').load('f{location}').limit(1)
#         schema=""
#         for i in df.dtypes:
#             schema=schema+i[0]+" "+i[1]+","
#         return schema[0:-1]
#     except Exception as err:
#         print("error occured") 

# COMMAND ----------

# def f_delta_cleansed_load(table_name,location,database):
#     try:
#         schema=pre_schema({location})
#         spark.sql(f"""
#                   create table if not exists {database}.{table_name}
#                   (f'{schema}')
#                   using delta
#                   location '{location}'
#                   """)
    
#     except Exception as err:
#         print("Error Occured ",str(err))

# COMMAND ----------

def pre_schema(location):
    try:
        # Corrected the string formatting here
        df = spark.read.format('delta').load(location).limit(1)
        schema = ""
        for field_name, field_type in df.dtypes:
            schema += f"{field_name} {field_type},"
        return schema[:-1]
    except Exception as err:
        print("Error occurred: ", str(err))

# COMMAND ----------

def f_delta_cleansed_load(table_name, location, database):
    try:
        schema = pre_schema(location)
        if schema:
            # Correct the SQL query syntax and use proper string interpolation
            query = f"""
                    CREATE TABLE IF NOT EXISTS {database}.{table_name} (
                        {schema}
                    )
                    USING DELTA
                    LOCATION '{location}'
                    """
            spark.sql(query)
        else:
            print("No schema found for the given location.")
    except Exception as err:
        print("Error Occurred: ", str(err))

# COMMAND ----------

def count_check(database, operation_type, table_name, number_diff):
        # Running the 'DESC HISTORY' command to get the historical data
        spark.sql(f"DESC HISTORY {database}.{table_name}").createOrReplaceTempView("table_count")

        # Getting the number of output rows for the current operation version
        count_current = spark.sql(f"""
        SELECT operationMetrics.numOutputRows
        FROM table_count
        WHERE version = (
            SELECT MAX(version)
            FROM table_count
            WHERE trim(lower(operation)) = lower('{operation_type}')
        )
        """).first()

        # Safeguard against None results
        final_count_current = 0 if count_current is None or count_current[0] is None else int(count_current[0])

        # Getting the number of output rows for the previous operation version
        count_previous = spark.sql(f"""
        SELECT operationMetrics.numOutputRows
        FROM table_count
        WHERE version = (
            SELECT MAX(version)
            FROM table_count
            WHERE version < (
                SELECT MAX(version)
                FROM table_count
                WHERE trim(lower(operation)) = lower('{operation_type}')
            )
        )
        """).first()

        # Safeguard against None results
        final_count_previous = 0 if count_previous is None or count_previous[0] is None else int(count_previous[0])

        print(f"Current count: {final_count_current}, Previous count: {final_count_previous}")

        # Checking the difference between current and previous counts
        if (final_count_current - final_count_previous) > number_diff:
            print(f"Difference is huge",table_name)
            raise Exception(f"Difference is huge",table_name)
        else:
            print(f"Counts are within the expected range",table_name)
    

# COMMAND ----------



# COMMAND ----------


