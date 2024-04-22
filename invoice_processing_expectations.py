# Databricks notebook source

# COMMAND

import dlt
import pyspark.sql.functions as F

# COMMAND


@dlt.table(temporary=True)
@dlt.expect_all_or_fail({"customer id in both tables": "counter = 0"})
def expect_customer_id_both_tables():
    customer_silver = dlt.read_stream("customer_silver")
    return (
        dlt.read_stream("invoices_silver")
        .join(customer_silver, on="customer_id", how="left")
        .select(customer_silver.customer_id)
        .filter(customer_silver.customer_id.isNull())
        .count()
        .alias("counter")
    )
