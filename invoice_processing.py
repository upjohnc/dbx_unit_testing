# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# COMMAND ----------


@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(
    {
        "customer_id_not_null": "customer_id is not NULL",
        "customer_name_more_than_one_char": "len(customer_name) > 1",
    }
)
def customer_silver():
    return dlt.read_stream("customer_bronze").select(
        F.col("CustomerID").cast("int").alias("customer_id"),
        F.col("CustomerName").alias("customer_name"),
    )


# COMMAND ----------

invoice_columns = [
    F.col("InvoiceNo").alias("invoice_no"),
    F.col("StockCode").alias("stock_code"),
    F.col("Description").alias("description"),
    F.col("Quantity").alias("quantity"),
    F.to_date("InvoiceDate", format="d-M-y H.m").alias("invoice_date"),
    F.col("UnitPrice").alias("unit_price"),
    F.col("CustomerID").alias("customer_id"),
    F.col("Country").alias("country"),
    F.year(F.to_date("InvoiceDate", format="d-M-y H.m")).alias("invoice_year"),
    F.month(F.to_date("InvoiceDate", format="d-M-y H.m")).alias("invoice_month"),
    # "load_time",
]


@dlt.table(table_properties={"quality": "silver"})
@dlt.expect_all_or_drop(
    {
        "invoice_no_not_null_and_number": "invoice_no is not NULL",
        "customer_id_not_null": "customer_id is not NULL",
        "quantity_non-negative": "quantity >= 0",
    }
)
def invoices_silver():
    return dlt.read_stream("invoices_bronze").select(invoice_columns)


# COMMAND ----------


@dlt.table(table_properties={"quality": "gold"})
def daily_sales_2022():
    return (
        dlt.read_stream("invoices_silver")
        .filter((F.col("invoice_year") == 2022) & (F.col("country") == "United Kingdom"))
        .groupby("country", "invoice_date")
        .agg(
            F.round(F.sum((F.col("quantity") * F.col("unit_price"))), 2).alias(
                "total_sales"
            )
        )
    )
