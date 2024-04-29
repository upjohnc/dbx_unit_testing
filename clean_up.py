# Databricks notebook source

catalog = "hive_metastore"
schema_name = "uc_001_basic_dlt_schema"
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
else:
    print(f"Schema Name '{schema_name}' does not exist")
