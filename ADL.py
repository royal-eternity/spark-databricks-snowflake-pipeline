# ADL.py
# Reads user score data from a Unity Catalog volume, aggregates scores per username,
# and saves the result as a Delta table for downstream joining.

from pyspark.sql.functions import *

# NOTE: replace this path with your own volume path
# (Data Ingestion -> Upload files to a volume -> copy the resulting path)
df = spark.read.parquet("/Volumes/workspace/default/myfiles/data.parquet")

aggdf = df.groupBy("username").agg(collect_list("score").alias("scores"))

spark.sql("CREATE DATABASE IF NOT EXISTS zeyodb")

aggdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "zeyodb.adl_tab"
)

print("== ADL DATA WRITTEN TO TABLE ==")

# Sanity check
spark.sql("SELECT * FROM zeyodb.adl_tab LIMIT 10").show()
