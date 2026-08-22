# MASTER.py
# Joins the three source tables (ADL, Snowflake, API) into one master table,
# writes the result to Databricks, then writes it back to Snowflake as
# MASTER_TAB.

import snowflake.connector
import pandas as pd
from pyspark.sql.functions import col, to_json

# ---------------------------------------------------------------------------
# 1. Load and join the source tables
# ---------------------------------------------------------------------------

adl_tab = spark.table("zeyodb.adl_tab")

# Normalize column names to lowercase for consistent joins
snow_tab = spark.table("zeyodb.snow_tab")
for col_name in snow_tab.columns:
    snow_tab = snow_tab.withColumnRenamed(col_name, col_name.lower())

api_tab = spark.table("zeyodb.api_tab").dropDuplicates(["username"])

# LEFT join to preserve all adl_tab rows even if snow_tab/api_tab have no matches
joindf = (
    adl_tab
    .join(snow_tab, ["username"], "left")
    .join(api_tab, ["username"], "left")
)

# ---------------------------------------------------------------------------
# 2. Write the joined result to Databricks as zeyodb.master_tab
# ---------------------------------------------------------------------------

spark.sql("DROP TABLE IF EXISTS zeyodb.master_tab")

joindf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "zeyodb.master_tab"
)

print("Master table written to Databricks")

# ---------------------------------------------------------------------------
# 3. Prepare the joined data for Snowflake
# ---------------------------------------------------------------------------

# Convert scores to JSON string if the column exists (array/complex types
# aren't natively supported by Snowflake's pandas writer)
if "scores" in joindf.columns:
    joindf_with_json = joindf.withColumn("scores", to_json(col("scores")))
else:
    joindf_with_json = joindf

# Collect Spark rows and convert to Pandas
rows = joindf_with_json.collect()
pandas_out = pd.DataFrame([row.asDict(recursive=True) for row in rows])

print("Number of rows:", len(pandas_out))
print("Number of columns:", len(pandas_out.columns))
print("Columns:", list(pandas_out.columns))

# Drop duplicate column names, if any
pandas_out = pandas_out.loc[:, ~pandas_out.columns.duplicated()]

# Make column names Snowflake-safe
clean_columns = []
for i, column in enumerate(pandas_out.columns):
    column = str(column).strip()
    if column == "":
        column = f"COLUMN_{i}"
    column = column.replace(" ", "_").replace("-", "_").replace(".", "_")
    clean_columns.append(column)

pandas_out.columns = clean_columns

# Ensure scores is a plain string column
if "scores" in pandas_out.columns:
    pandas_out["scores"] = pandas_out["scores"].astype(str)

print("Final columns:")
print(list(pandas_out.columns))

if len(pandas_out.columns) == 0:
    raise ValueError("The DataFrame has no columns. Cannot create MASTER_TAB.")

# ---------------------------------------------------------------------------
# 4. Connect to Snowflake and write MASTER_TAB
# ---------------------------------------------------------------------------

# Password is entered at runtime (Databricks widget)
dbutils.widgets.text("snowflake_password", "")
snowflake_password = dbutils.widgets.get("snowflake_password").strip()

conn = snowflake.connector.connect(
    account="NUIQHMR-BF96127",
    user="ROYALCHODAGIR",
    password=snowflake_password,
    database="zeyodb",
    schema="zeyoschema",
    warehouse="COMPUTE_WH",
)

print("Connected to Snowflake successfully")

from snowflake.connector.pandas_tools import write_pandas

success, nchunks, nrows, output = write_pandas(
    conn,
    pandas_out,
    "MASTER_TAB",
    auto_create_table=True,
    overwrite=True,
    quote_identifiers=True,
)

print("Success:", success)
print("Chunks:", nchunks)
print("Rows written:", nrows)

conn.close()

print("Master table written to Snowflake successfully")

# ---------------------------------------------------------------------------
# 5. Sanity check
# ---------------------------------------------------------------------------

spark.sql("SELECT * FROM zeyodb.master_tab LIMIT 10").show()
