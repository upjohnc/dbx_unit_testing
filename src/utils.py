from typing import List, Optional, Union

import pyspark.sql.functions as F
import pytest
from pyspark.sql import Column, DataFrame


def columns_except(
    df: DataFrame, except_columns: Optional[List[str]], as_col_string: bool = False
) -> List[Union[str, Column]]:
    """Return all columns except some

    An "selet *" for when you want to drop some columns rather than typing out all the columns you want.

    Args:
        df (DataFrame): Spark DataFrame of the data you want to columns from
        except_columns (Optional[List[str]]): columns that you don't want in the columns
        as_col_string (bool): return the columns as a string representation otherwise returned as Column types

    Returns:
        List[Union[str, Column]]: list of columns that you want from the dataframe
    """

    if except_columns is None:
        except_columns = []

    return [
        F.col(i) if as_col_string is False else i
        for i in df.columns
        if i not in except_columns
    ]


@pytest.mark.usefixtures("spark_session")
class TestAllSpark:
    def test_columns_as_strings(self, spark_session):
        # Given
        original_df = spark_session.createDataFrame(
            [[1, 2, 3, 4]], schema="col1 int, col2 int, col3 int, col4 int"
        )

        # When
        new_cols = columns_except(original_df, ["col2", "col4"], as_col_string=True)

        # Then
        assert new_cols == ["col1", "col3"]

    def test_columns_as_col(self, spark_session):
        # Given
        original_df = spark_session.createDataFrame(
            [[1, 2, 3, 4]], schema="col1 int, col2 int, col3 int, col4 int"
        )
        # When
        new_cols = columns_except(original_df, ["col2", "col4"])

        # Then
        expected = [F.col("col1"), F.col("col3")]
        expected_assertion = [str(i) for i in expected]
        assert [str(i) for i in new_cols] == expected_assertion
