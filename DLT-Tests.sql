CREATE TEMPORARY LIVE TABLE TEST_user_bronze_dlt (
  CONSTRAINT incorrect_data_removed EXPECT (not_empty_rescued_data = 0) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: bronze table properly drops row with incorrect schema"
AS
SELECT count(*) AS not_empty_rescued_data
FROM live.user_bronze_dlt
WHERE _rescued_data IS NOT NULL OR email = 'margaret84@example.com'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Let's continue our tests on the silver table with multiple checks at once
-- MAGIC
-- MAGIC We'll next ensure that our silver table transformation does the following:
-- MAGIC
-- MAGIC * null ids are removed (our test dataset contains null)
-- MAGIC * we should have 4 rows as output (based on the input)
-- MAGIC * the emails are properly anonymized

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_silver_dlt_anonymize (
  CONSTRAINT keep_all_rows              EXPECT (num_rows = 4)      ON VIOLATION FAIL UPDATE,
  CONSTRAINT email_should_be_anonymized EXPECT (clear_email = 0)  ON VIOLATION FAIL UPDATE,
  CONSTRAINT null_ids_removed           EXPECT (null_id_count = 0) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: check silver table removes null ids and anonymize emails"
AS (
  WITH rows_test  AS (
      SELECT count(*) AS num_rows
      FROM LIVE.user_silver_dlt
    ),
  email_test AS (
      SELECT count(*) AS clear_email
      FROM live.user_silver_dlt
      WHERE email LIKE '%@%'
     ),
  id_test    AS (
      SELECT count(*) AS null_id_count
      FROM live.user_silver_dlt
      WHERE id IS NULL
     )
  SELECT * from email_test, id_test, rows_test
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testing Primary key uniqueness
-- MAGIC
-- MAGIC Finally, we'll enforce uniqueness on the gold table to avoid any duplicates

-- COMMAND ----------

CREATE TEMPORARY LIVE TABLE TEST_user_gold_dlt (
  CONSTRAINT pk_must_be_unique EXPECT (duplicate = 1) ON VIOLATION FAIL UPDATE
)
COMMENT "TEST: check that gold table only contains unique customer id"
AS
SELECT count(*) AS duplicate, id
FROM LIVE.user_gold_dlt
GROUP BY id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC That's it. All we have to do now is run the full pipeline.
-- MAGIC
-- MAGIC If one of the condition defined in the TEST table fail, the test pipeline expectation will fail and we'll know something need to be fixed!
-- MAGIC
-- MAGIC You can open the <a dbdemos-pipeline-id="dlt-test" href="#joblist/pipelines/cade4f82-4003-457c-9f7c-a8e5559873b6">Delta Live Table Pipeline for unit-test</a> to see the tests in action
