import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="class")
def spark_session():
    return SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
