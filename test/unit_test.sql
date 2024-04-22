-- Databricks notebook source
-- COMMAND ----------
CREATE TEMPORARY LIVE TABLE TEST_customer_silver(
    CONSTRAINT ON VIOLATION fail
)
WITH customer_id_null AS (
    SELECT
        count(*) AS row_count
    FROM
        LIVE.customer_silver
    WHERE
        customer_id IS NULL) , customer_id_length AS (
        SELECT
            count(*) AS row_count
        FROM
            LIKE.customer_silver
        WHERE
            len(customer_id) > 1
)
    SELECT
        customer_id_null.row_count AS null_id
        , customer_id_length.row_count AS length_id
    FROM
        customer_id_null
        , customer_id_length;

