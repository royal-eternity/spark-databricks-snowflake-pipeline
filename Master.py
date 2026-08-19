# Databricks notebook source
# Master.py
# Joins the three source tables into one, then writes the result to Databricks
# and back to Snowflake.

adl_tab = spark.table("zeyodb.adl_tab")
snow_tab = spark.table("zeyodb.snow_tab")
api_tab = spark.table("zeyodb.api_tab").dropDuplicates(["username"])

joindf = (
    adl_tab
    .join(snow_tab, ["username"], "inner")
    .join(api_tab, ["username"], "left")
)

joindf.display()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS zeyodb.master_tab")

joindf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("zeyodb.master_tab")

print("Master table written to Databricks")

# COMMAND ----------

%pip install snowflake-connector-python

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Restarting Python clears memory, so the tables and join are rebuilt here
adl_tab = spark.table("zeyodb.adl_tab")
snow_tab = spark.table("zeyodb.snow_tab")
api_tab = spark.table("zeyodb.api_tab").dropDuplicates(["username"])

joindf = (
    adl_tab
    .join(snow_tab, ["username"], "inner")
    .join(api_tab, ["username"], "left")
)

# COMMAND ----------

import snowflake.connector
import pandas as pd
from pyspark.sql.functions import col, to_json

# Password is entered here at run time, not stored in this file.
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password")

joindf_with_json = joindf.withColumn("scores", to_json(col("scores")))

# Collecting rows manually here avoids a known .toPandas() issue on
# Databricks Serverless compute.
rows = joindf_with_json.collect()
pandas_out = pd.DataFrame([r.asDict() for r in rows])

conn = snowflake.connector.connect(
    account="<your_snowflake_account>",
    user="<your_snowflake_username>",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH",
    role="ACCOUNTADMIN"
)

from snowflake.connector.pandas_tools import write_pandas
success, nchunks, nrows, _ = write_pandas(
    conn,
    pandas_out,
    "MASTER_TAB",
    auto_create_table=True,
    overwrite=True
)

conn.close()

print(f"Master table written to Snowflake - success: {success}, rows: {nrows}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zeyodb.master_tab LIMIT 10
