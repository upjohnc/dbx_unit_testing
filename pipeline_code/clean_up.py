# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline and Job Resource Clean Up

# MAGIC This notebook cleans up the tables and schema
# MAGIC created by the DLT pipeline of the unit tests.

# COMMAND ----------
catalog = "hive_metastore"
schema_name = "uc_001_basic_dlt_schema"

# Check that the schema exists
if (
    spark.sql(f"SHOW SCHEMAS IN {catalog}")
    .filter(f"databaseName == '{schema_name}'")
    .count()
    == 1
):
    tables = spark.sql(f"SHOW TABLES in {schema_name};").collect()

    for table in tables:
        spark.sql(f"DROP TABLE {schema_name}.{table.tableName};")

    spark.sql(f"drop schema {schema_name};")

# If schema doesn't exist then inform the user.
else:
    print(f"Schema Name '{schema_name}' does not exist")
