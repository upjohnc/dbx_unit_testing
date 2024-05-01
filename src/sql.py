"""
Script to check failures in Delta Live Table Expectations.

For tables that have an expectation of warn or drop row,
the record of pass and fails is stored in the events log.
The script queries for those logged events and reports
the expectation names of the failed events.

The script can be repurposed for other reporting purposes.
"""

import os
import sys

from databricks import sql


def get_connection_config() -> dict:
    token = os.environ.get("TOKEN")
    server = os.environ.get("DBX_SERVER")

    if token is None:
        print("Need access token")
        sys.exit(1)
    return dict(
        server_hostname=server,
        http_path="/sql/1.0/warehouses/0b83bc4c230ceef2",
        access_token=token,
    )


def get_events_log() -> str:
    return """
            SELECT
                *
            FROM
                delta. `dbfs:/mnt/dbacademy-datasets/dbx_unit_tests/system/events`
            """


def get_schema() -> list[dict]:
    return [
        {
            "name": "str",
            "dataset": "str",
            "passed_records": "int",
            "failed_records": "int",
        }
    ]


def get_most_recent_timestamp() -> str:
    return """
           SELECT
               max(timestamp) AS timestamp
           FROM
               events_log
           WHERE
               origin.update_id IS NOT NULL
        """


def get_failure_records(input_data) -> list[str]:
    return [
        i.expectation["name"]
        for i in input_data
        if int(i.expectation["failed_records"]) > 0
    ]


def build_query() -> str:
    query = f"""
             WITH events_log AS ({get_events_log()})
             , most_recent_expectations AS (
                 SELECT
                     origin.update_id
                     , explode(from_json(details:flow_progress:data_quality:expectations
                             , schema_of_json("{get_schema()}"))) AS expectation
                 FROM
                     events_log
                 WHERE
                     details:flow_progress.metrics IS NOT NULL
                     AND origin.update_id = (
                         SELECT
                             origin.update_id AS update_id
                         FROM
                             events_log
                         WHERE ({get_most_recent_timestamp()}) = timestamp
                         )
               )
             SELECT
                 *
             FROM
                 most_recent_expectations
             WHERE
                 expectation.dataset rlike 'TEST.*';
        """
    return query


def main():
    query = build_query()

    with sql.connect(**get_connection_config()) as connection_:
        with connection_.cursor() as cursor_:
            cursor_.execute(query)
            result = cursor_.fetchall()

    # Check that the query filter is set correctly - expect some records
    if len(result) == 0:
        print("No expectation records retrieved")
        sys.exit(1)

    failure_records = get_failure_records(result)
    if len(failure_records) > 0:
        print("Failed Expectations in Unit Tests:")
        for i in failure_records:
            print("-", i)
        sys.exit(1)

    print("All good")


if __name__ == "__main__":
    main()


# Unit tests of script
class Row:
    def __init__(self, update_id, expectation):
        self.update_id = update_id
        self.expectation = expectation


def test_failed_expectations():
    expectation_name = "no_null_invoice_id"
    input_ = [
        Row(
            update_id="0379bcf8-68a6-4fdf-b2c2-d9af0783bf5f",
            expectation={
                "dataset": "TEST_invoice_silver",
                "failed_records": "1",
                "name": expectation_name,
                "passed_records": "1",
            },
        ),
    ]
    result = get_failure_records(input_)
    assert expectation_name in result


def test_success_expectations():
    expectation_name = "no_null_invoice_id"
    input_ = [
        Row(
            update_id="0379bcf8-68a6-4fdf-b2c2-d9af0783bf5f",
            expectation={
                "dataset": "TEST_invoice_silver",
                "failed_records": "0",
                "name": expectation_name,
                "passed_records": "1",
            },
        ),
    ]
    result = get_failure_records(input_)
    assert len(result) == 0
