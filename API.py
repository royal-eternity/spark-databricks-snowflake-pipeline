# Databricks notebook source
# API.py
# Pulls sample user profile data from a public API and saves it for the join.
# No credentials needed for this one - randomuser.me is a free public API.

import urllib.request
from pyspark.sql.functions import *

url = "https://randomuser.me/api/0.8/?results=1000"

urldata = urllib.request.urlopen(url).read().decode("utf-8")

schema = spark.range(1).select(schema_of_json(lit(urldata))).first()[0]

jsondf = spark.createDataFrame([(urldata,)], ["json_data"]) \
              .select(from_json(col("json_data"), schema).alias("data")) \
              .select("data.*")

final_df = jsondf.withColumn("results", explode("results")).selectExpr(
    "nationality",
    "results.user.gender as gender",
    "results.user.name.title as title",
    "results.user.name.first as first",
    "results.user.name.last as last",
    "results.user.location.street as street",
    "results.user.location.city as city",
    "results.user.location.state as state",
    "results.user.location.zip as zip",
    "results.user.email as email",
    "results.user.username as username",
    "results.user.password as password",
    "results.user.salt as salt",
    "results.user.md5 as md5",
    "results.user.sha1 as sha1",
    "results.user.sha256 as sha256",
    "results.user.registered as registered",
    "results.user.dob as dob",
    "results.user.phone as phone",
    "results.user.cell as cell",
    "results.user.picture.large as large",
    "results.user.picture.medium as medium",
    "results.user.picture.thumbnail as thumbnail",
    "seed",
    "version"
)

removenum = final_df.withColumn("username", expr("regexp_replace(username,'[0-9]', '')"))

# randomuser.me repeats usernames often, so dedupe here to keep the later join clean
deduped = removenum.dropDuplicates(["username"])

deduped.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("zeyodb.api_tab")

print("API data written to zeyodb.api_tab")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM zeyodb.api_tab LIMIT 10
