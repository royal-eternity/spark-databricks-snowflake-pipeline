# SNOW.py
# Pulls source data from Snowflake into a Pandas DataFrame, converts it to
# Spark, and saves the result as a Delta table for downstream joining.

import snowflake.connector
import pandas as pd

# Password is entered at runtime (Databricks widget)
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password")

conn = snowflake.connector.connect(
    account="NUIQHMR-BF96127",
    user="ROYALCHODAGIR",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH",
)

pandas_df = pd.read_sql("SELECT * FROM snow_source", conn)
conn.close()

snow_df = spark.createDataFrame(pandas_df)

snow_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "zeyodb.snow_tab"
)

print("== SNOWFLAKE DATA WRITTEN TO TABLE ==")
