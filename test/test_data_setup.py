# Databricks notebook source
# MAGIC %md
# MAGIC # Test Datasets setup
# MAGIC
# MAGIC We have 2 files that we'll be using as dataset saved in git within the project (you can open them directly under the `dataset` folder).
# MAGIC
# MAGIC All we have to do is move these local files to our blob storage so that we can read them within our DLT test pipeline.
# MAGIC
# MAGIC *Note: We could also have used Faker to generate them dynamicall.*
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdlt_unit_test%2Fnotebook_dataset&dt=DLT_UNIT_TEST">

# COMMAND ----------

import dlt

# CustomerID
# CustomerName


# InvoiceNo
# StockCode
# Description	Quantity	InvoiceDate	UnitPrice	CustomerID	Country

# COMMAND ----------

customer_data = [
    {"CustomerID": "invalid_id", "CustomerName": "Big Name"},
    {"CustomerID": None, "CustomerName": "Nice Name"},
    {"CustomerID": 1, "CustomerName": "A"},
    {"CustomerID": 2, "CustomerName": "Mr Smooth"},
    {"CustomerID": 4, "CustomerName": "Mr Hammond"},
]


@dlt.table(
    comment="Fake data of customers for testing",
    temporary=True,
)
def customer_raw():
    return spark.createDataFrame(customer_data)


# COMMAND ----------
invoice_data = [
    # invoiceid
    # invalid_id
    # null id
    # no customerid
    # quantity -2
    {
        "InvoiceNo": 536392,
        "StockCode": "21891",
        "Description": "VINTAGE BILLBOARD DRINK ME MUG",
        "Quantity": -2,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 1.06,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": None,
        "StockCode": "21889",
        "Description": "VINTAGE BILLBOARD LOVE/HATE MUG",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 1.06,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536392,
        "StockCode": "22827",
        "Description": "WOOD 2 DRAWER CABINET WHITE FINISH",
        "Quantity": 4,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 4.95,
        "CustomerID": None,
        "Country": "United Kingdom",
    },
    # command
    # invoice_data
    # invoiceid
    # 1, 1, 1
    # stockid
    # 1,2,3
    # customerid
    # 2,2,6
    {
        "InvoiceNo": 536394,
        "StockCode": "85152",
        "Description": "CREAM CUPID HEARTS COAT HANGER",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 2.75,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536394,
        "StockCode": "85123A",
        "Description": "RETRO COFFEE MUGS ASSORTED",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 1.06,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536394,
        "StockCode": "22652",
        "Description": "SAVE THE PLANET MUG",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 1.06,
        "CustomerID": 19860,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536627,
        "StockCode": "22699",
        "Description": "ROSES REGENCY TEACUP AND SAUCER ",
        "Quantity": 12,
        "InvoiceDate": "02-01-2022 10.53",
        "UnitPrice": 2.95,
        "CustomerID": 15658,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536627,
        "StockCode": "21755",
        "Description": "LOVE BUILDING BLOCK WORD",
        "Quantity": 4,
        "InvoiceDate": "02-01-2022 10.53",
        "UnitPrice": 5.95,
        "CustomerID": 15658,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536628,
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 2.55,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
    {
        "InvoiceNo": 536628,
        "StockCode": "71053",
        "Description": "WHITE METAL LANTERN",
        "Quantity": 6,
        "InvoiceDate": "07-01-2022 10.54",
        "UnitPrice": 3.39,
        "CustomerID": 17850,
        "Country": "United Kingdom",
    },
]


@dlt.table(
    comment="Fake data of invoice for testing",
    temporary=True,
)
def invoice_raw():
    return spark.createDataFrame(invoice_data)
