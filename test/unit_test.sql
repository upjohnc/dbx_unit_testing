-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Unit Tests
-- MAGIC
-- MAGIC Expectations to test the silver and gold tier queries handled the business rules as defined.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Customer Silver
-- MAGIC
-- MAGIC Check that 
-- MAGIC - customer_id is not null
-- MAGIC - length of customer_name is greater than 1

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_customer_silver(
    CONSTRAINT no_nulls EXPECT(null_id = 0) ON VIOLATION FAIL UPDATE
    , CONSTRAINT length_greater_than_1 EXPECT(length_id = 0) ON VIOLATION FAIL UPDATE
)
WITH customer_id_null AS (
    SELECT
        count(*) AS row_count
    FROM
        LIVE.customer_silver
    WHERE
        customer_id IS NULL),
customer_id_length AS (
        SELECT
            count(*) AS row_count
        FROM
            LIVE.customer_silver
        WHERE
            len(customer_id) > 1
)
    SELECT
        customer_id_null.row_count AS null_id
        , customer_id_length.row_count AS length_id
    FROM
        customer_id_null
        , customer_id_length;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test Invoices Silver
-- MAGIC
-- MAGIC Check that:
-- MAGIC - invoice_no is not null
-- MAGIC - invoice_no is a number
-- MAGIC - quantity is non-negative

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_invoice_silver(
    CONSTRAINT no_null_cust_id expect(customer_id_count = 0) ON VIOLATION fail UPDATE
    , CONSTRAINT no_null_invoice_id expect(invoice_no_null_count = 0) ON VIOLATION fail UPDATE
    , CONSTRAINT number_invoice_id expect(invoice_no_number_count = 0) ON VIOLATION fail UPDATE
    , CONSTRAINT quantity_greater_than_zero expect(quantity_count = 0) ON VIOLATION fail UPDATE
)
WITH customer_id_null AS (
    SELECT
        count(*) AS row_count
    FROM
        LIVE.invoices_silver
    WHERE
        customer_id IS NULL) 
, invoice_no_null AS (
    SELECT
        count(*) AS row_count
    FROM
        LIVE.invoices_silver
    WHERE
        invoice_no IS NULL)
, invoice_no_number AS (
    WITH temp AS (
    SELECT
        CASE WHEN TRY_CAST(invoice_no AS int) IS NULL THEN
            0
        ELSE
            1
        END AS is_numeric
    FROM
        LIVE.invoices_silver
    )
        SELECT
            count(*) AS row_count
        FROM
            temp
        WHERE
            is_numeric == 0)
, quantity_negative AS (
    SELECT
        count(*) AS row_count
    FROM
        LIVE.invoices_silver
    WHERE
        quantity < 1
)
SELECT
    customer_id_null.row_count AS customer_id_count
    , invoice_no_null.row_count AS invoice_no_null_count
    , invoice_no_number.row_count AS invoice_no_number_count
    , quantity_negative.row_count AS quantity_count
FROM
    customer_id_null
    , invoice_no_null
    , invoice_no_number
    , quantity_negative;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Test the Gold UK Aggregation
-- MAGIC
-- MAGIC Check that 2 rows are created from the aggregation based on the test data

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_uk_aggregation(
    CONSTRAINT aggregation_count EXPECT(row_count = 2) ON VIOLATION fail UPDATE
)
SELECT
    count (*) AS row_count
FROM
    LIVE.daily_sales_uk_2022;
