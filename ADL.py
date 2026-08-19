# Databricks notebook source
# ADL.py
# Reads user score data from a Unity Catalog volume and aggregates scores per username.

from pyspark.sql.functions import *

# Replace this with your own volume path.
# (In Databricks: New -> Add or upload data -> Upload files to a volume -> copy the path it gives you)
df = spark.read.parquet("/Volumes/workspace/default/<your_volume_name>/data.parquet")

aggdf = df.groupBy("username").agg(collect_list("score").alias("scores"))

spark.sql("CREATE DATABASE IF NOT EXISTS zeyodb")

aggdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("zeyodb.adl_tab")

print("ADL data written to zeyodb.adl_tab")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zeyodb.adl_tab LIMIT 10
