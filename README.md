# Zeyo Data Pipeline

An end-to-end data pipeline that pulls user data from three different sources,
joins it into one table, and writes the result to both Databricks and Snowflake.
Built to practice integrating PySpark, Databricks, and Snowflake.

## Architecture

```
   Parquet file          Public API           Snowflake table
 (score data)         (profile data)          (IP/session data)
        │                    │                        │
        ▼                    ▼                        ▼
     ADL.py               API.py                   Snow.py
   (Spark read,        (Spark read,            (Snowflake read,
   aggregate)            dedupe)                 Spark convert)
        │                    │                        │
        └────────────────────┼────────────────────────┘
                              ▼
                          Master.py
                    (join on username)
                              │
                ┌─────────────┴─────────────┐
                ▼                            ▼
        Databricks table              Snowflake table
        zeyodb.master_tab              MASTER_TAB
```

Each source is read and saved as its own table, then `Master.py` joins all
three on `username` and writes the combined result to both Databricks and
Snowflake.

## Sources

| Source | Notebook | What it provides |
|---|---|---|
| Parquet file (Unity Catalog volume) | `ADL.py` | Per-user scores, aggregated into a list |
| Public API (randomuser.me) | `API.py` | Sample user profile data (name, email, location, etc.) |
| Snowflake table | `Snow.py` | IP/session data |

## Tech used

- PySpark
- Databricks (Free Edition)
- Snowflake (free trial)
- Unity Catalog volumes for file storage
- Snowflake Python connector (Databricks Free Edition's Serverless compute
  doesn't include the Spark-Snowflake JDBC driver, so the Python connector +
  pandas is used instead)

## Setup

You'll need:
- A free Databricks account ([databricks.com/try-databricks](https://databricks.com/try-databricks) - Community/Free Edition)
- A free Snowflake trial account ([signup.snowflake.com](https://signup.snowflake.com))

### 1. Snowflake

Create a database, schema, and a source table to represent the IP/session data:

```sql
CREATE DATABASE IF NOT EXISTS zeyodb;
CREATE SCHEMA IF NOT EXISTS zeyodb.zeyoschema;

CREATE OR REPLACE TABLE zeyodb.zeyoschema.snow_source (
    username STRING,
    ip STRING
);

INSERT INTO zeyodb.zeyoschema.snow_source VALUES
('sample_user1', '192.168.1.10'),
('sample_user2', '192.168.1.11');
```

Use real usernames that also exist in your parquet file, or the join in
`Master.py` will return no rows.

### 2. Databricks

1. Upload `sample_data/data.parquet` to a Unity Catalog volume
   (**New -> Add or upload data -> Upload files to a volume**), and copy the
   resulting path.
2. Import the four notebooks from `notebooks/` into your workspace.
3. In `ADL.py`, replace the parquet path with the one you copied.
4. In `Snow.py` and `Master.py`, replace `<your_snowflake_account>` and
   `<your_snowflake_username>` with your own Snowflake account identifier and
   username.
5. Run the notebooks in this order: `ADL.py` -> `API.py` -> `Snow.py` -> `Master.py`.
6. `Snow.py` and `Master.py` will prompt for a `snowflake_password` widget at
   the top of the notebook when run - enter your Snowflake password there.
   It is never stored in the code.

## Sample output

_(screenshot of the final master_tab query result goes here)_

## Planned improvements

This is a working pipeline, not a production system. With more time I'd add:

- A proper bronze/silver/gold layering structure with a data quality layer
  (null checks, duplicate checks, schema validation) between stages
- Automated scheduling via a Databricks Job instead of running notebooks manually
- Logging instead of print statements
- Basic unit tests for the transformation logic
- Spark performance tuning (partitioning, broadcast joins) once the data
  volume is large enough to need it
