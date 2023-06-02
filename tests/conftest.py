from pyspark import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def glueContext():
    """
    Function to setup test environment for PySpark and Glue
    """
    spark_context = SparkContext()
    glueContext = GlueContext(spark_context)
    yield glueContext
    spark_context.stop()

@pytest.fixture(scope="session")
def spark():
    """
    Function to setup test environment for PySpark and Glue
    """
    spark = SparkSession.builder.appName("test").getOrCreate()
    yield spark
    spark.stop()



