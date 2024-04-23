# Databricks notebook source
# MAGIC %md
# MAGIC # Production - Data Validation
# MAGIC
# MAGIC Leverage temp tables to validate the data in silver and gold tiers
# MAGIC

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# COMMAND ----------


@dlt.table(
    temporary=True,
    comment="Customer_id that are in invoices_silver are also in customers_silver",
)
@dlt.expect_all_or_fail({"customer id in both tables": "counter = 0"})
def expect_customer_id_both_tables():
    customer_silver = dlt.read_stream("customer_silver")
    return (
        dlt.read_stream("invoices_silver")
        .join(customer_silver, on="customer_id", how="left")
        .select(customer_silver.customer_id)
        .filter(customer_silver.customer_id.isNull())
        .agg(F.count("*").alias("counter"))
    )


# COMMAND ----------


@dlt.table(
    temporary=True, comment="test that combined invoice_no and stock_code are unique"
)
@dlt.expect_all_or_fail({"unique_pk_invoice_id_stock_code": "counter < 2"})
def invoice_no_stock_code_pk():
    return (
        dlt.read_stream("invoices_silver")
        .groupby("invoice_no", "stock_code")
        .agg(F.count("*").alias("counter"))
    )


# COMMAND ----------


@dlt.table(temporary=True, comment="For each invoice no there is only one customer id")
@dlt.expect_all_or_fail({"single_customer_per_invoice_no": "counter < 2"})
def one_customer_id_per_invoice_no():
    df = dlt.read_stream("invoices_silver").select("invoice_no", "customer_id").distinct()

    return df.groupby("invoice_no").agg(F.count("*").alias("counter"))
