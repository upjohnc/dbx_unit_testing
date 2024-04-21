# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------


@dlt.table()
@dlt.expect_all_or_drop(
    {
        "customer_id_not_null": "customer_id is not NULL",
        "customer_name_more_than_one_char": "len(customer_name) > 1",
    }
)
def customer_bronze():
    return dlt.read_stream("customer_raw").select(
        F.col("CustomerID").cast("int").alias("customer_id"),
        F.col("CustomerName").alias("customer_name"),
    )


# COMMAND ----------


@dlt.table
@dlt.expect_all_or_drop({"invoice_no_not_null": "InvoiceNo is not NULL"})
def invoices_bronze():
    return dlt.read_stream("invoices_raw")
