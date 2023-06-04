from pyspark import SparkContext
from awsglue.context import GlueContext
from etl.mock.infrastructure.s3_resource import S3ResourceSingleton
import pytest


@pytest.fixture(scope="function")
def s3_resource():
    # Ensure teardown was completed previously and setup.
    if S3ResourceSingleton._instance:
        print("Teardown was not completed correctly in an earlier test.")
        S3ResourceSingleton.teardown()
        
    s3_resource_instance = S3ResourceSingleton.getMockInstance()

    # Use.
    yield s3_resource_instance

    # Teardown.
    S3ResourceSingleton.teardown()


@pytest.fixture(scope="session")
def glueContext():
    """
    Function to setup test environment for PySpark and Glue
    """
    spark_context = SparkContext()
    glueContext = GlueContext(spark_context)
    yield glueContext
    spark_context.stop()


def pytest_addoption(parser):
    parser.addoption("--integration", action="store_true", help="run the tests only in case of that command line (marked with marker @integration)")

def pytest_runtest_setup(item):
    if 'integration' in item.keywords and not item.config.getoption("--integration"):
        pytest.skip("requires --integration")

