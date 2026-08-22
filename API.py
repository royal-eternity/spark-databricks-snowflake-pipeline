# API.py
# Pulls sample user data from the randomuser.me API, flattens the JSON response,
# unions in a small set of shared/reference test records, and writes the
# combined result to a Delta table for downstream joining.

import urllib.request
from pyspark.sql import functions as F
from pyspark.sql import Row

url = "https://randomuser.me/api/0.8/?results=1000"

urldata = urllib.request.urlopen(url).read().decode("utf-8")

schema = spark.range(1).select(F.schema_of_json(F.lit(urldata))).first()[0]

jsondf = (
    spark.createDataFrame([(urldata,)], ["json_data"])
    .select(F.from_json(F.col("json_data"), schema).alias("data"))
    .select("data.*")
)

final_df = jsondf.withColumn("results", F.explode("results")).selectExpr(
    "nationality",
    "results.user.gender as gender",
    "results.user.name.title as title",
    "results.user.name.first as first",
    "results.user.name.last as last",
    "results.user.location.street as street",
    "results.user.location.city as city",
    "results.user.location.state as state",
    "CAST(results.user.location.zip AS STRING) as zip",
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
    "version",
)

removenum = final_df.withColumn(
    "username",
    F.lower(F.expr("regexp_replace(username,'[0-9]', '')")),
)

# Small set of shared/reference test records unioned in alongside the API pull
shared_api = spark.createDataFrame(
    [
        Row(
            username="testuser1", nationality="US", gender="male", title="Mr",
            first="Test", last="UserOne", street="123 Main St", city="Bengaluru",
            state="KA", zip="560001", email="testuser1@example.com",
            password="placeholder", salt="placeholder", md5="placeholder",
            sha1="placeholder", sha256="placeholder", registered=1600000000,
            dob=946684800, phone="000-000-0001", cell="000-000-0001",
            large="", medium="", thumbnail="", seed="seed1", version="0.8",
        ),
        Row(
            username="testuser2", nationality="US", gender="female", title="Ms",
            first="Test", last="UserTwo", street="124 Main St", city="Bengaluru",
            state="KA", zip="560002", email="testuser2@example.com",
            password="placeholder", salt="placeholder", md5="placeholder",
            sha1="placeholder", sha256="placeholder", registered=1600000001,
            dob=946684801, phone="000-000-0002", cell="000-000-0002",
            large="", medium="", thumbnail="", seed="seed2", version="0.8",
        ),
        Row(
            username="testuser3", nationality="US", gender="male", title="Mr",
            first="Test", last="UserThree", street="125 Main St", city="Bengaluru",
            state="KA", zip="560003", email="testuser3@example.com",
            password="placeholder", salt="placeholder", md5="placeholder",
            sha1="placeholder", sha256="placeholder", registered=1600000002,
            dob=946684802, phone="000-000-0003", cell="000-000-0003",
            large="", medium="", thumbnail="", seed="seed3", version="0.8",
        ),
        Row(
            username="testuser4", nationality="US", gender="female", title="Ms",
            first="Test", last="UserFour", street="126 Main St", city="Bengaluru",
            state="KA", zip="560004", email="testuser4@example.com",
            password="placeholder", salt="placeholder", md5="placeholder",
            sha1="placeholder", sha256="placeholder", registered=1600000003,
            dob=946684803, phone="000-000-0004", cell="000-000-0004",
            large="", medium="", thumbnail="", seed="seed4", version="0.8",
        ),
    ]
)

shared_api = shared_api.select(removenum.columns)

api_final = removenum.unionByName(shared_api)

spark.sql("CREATE DATABASE IF NOT EXISTS zeyodb")

api_final.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("zeyodb.api_tab")

print("== API DATA WRITTEN TO TABLE ==")

# Sanity check: confirm the shared/reference test records made it into the table
spark.table("zeyodb.api_tab").filter(
    F.col("username").isin("testuser1", "testuser2", "testuser3", "testuser4")
).show(20, False)
