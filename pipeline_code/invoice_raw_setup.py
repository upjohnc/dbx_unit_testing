# Databricks notebook source

# MAGIC %md
# MAGIC # Import to bronze tables


# COMMAND
@dlt.table(
    comment="Store the raw data ingested from Customers source.",
    table_properties={"quality": "bronze"},
)
def customers_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.format", "csv")
        .load(
            "/Volumes/devlab_accelerators_catalog/uc_001_basic_dlt_schema/landing_zone/customers/"
        )
        .select("*")
        .withColumn("load_time", F.current_timestamp())
    )


# COMMAND ----------
@dlt.table(
    comment="Store the raw data ingested from Invoices source.",
    table_properties={"quality": "bronze"},
)
def invoices_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.format", "csv")
        .load(
            "/Volumes/devlab_accelerators_catalog/uc_001_basic_dlt_schema/landing_zone/invoices/"
        )
        .select("*")
        .withColumn("load_time", F.current_timestamp())
    )
