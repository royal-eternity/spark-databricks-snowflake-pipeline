# Databricks notebook source
# Snow.py
# Reads a table from Snowflake and saves it locally for the join.
# Uses the Snowflake Python connector, since Databricks Free Edition (Serverless)
# doesn't ship the Spark-Snowflake JDBC driver.

# COMMAND ----------

%pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import snowflake.connector
import pandas as pd

# Password is entered here at run time, not stored in this file.
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password")

conn = snowflake.connector.connect(
    account="<your_snowflake_account>",
    user="<your_snowflake_username>",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH"
)

pandas_df = pd.read_sql("SELECT * FROM snow_source", conn)
conn.close()

snow_df = spark.createDataFrame(pandas_df)

snow_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("zeyodb.snow_tab")

print("Snowflake data written to zeyodb.snow_tab")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zeyodb.snow_tab LIMIT 10
