import pytest
import tempfile
import os
from etl.jobs.landing.claim import read_data, transform_data, write_data
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

class TestReadDataLogic:

    @pytest.fixture
    def spark(self):
        spark = SparkSession.builder.getOrCreate()
        return spark
    
    @pytest.fixture
    def sample_data_path(self):
        # Create a temporary sample CSV file for testing
        with tempfile.NamedTemporaryFile(delete=False) as csv_file:
            csv_file.write(b"name,age\nJohn,30\nAlice,25\n")
            # | name | age |
            # | John | 30  |
            # | Alice| 25  |
            temp_file_path = csv_file.name
        
        yield temp_file_path
        # Clean up the temporary file after the test
        os.remove(temp_file_path)

    # pytest -m "not slow" to exclude.
    @pytest.mark.slow
    def test_valid_read_data_with_header(self, spark,sample_data_path):
        schema = StructType().add("name", StringType()).add("age", IntegerType())
        df = read_data(spark, sample_data_path, schema)
        
        assert df.count() == 2
        assert df.schema == schema
        assert df.columns == ["name", "age"]
        assert df.collect() == [("John", 30), ("Alice", 25)]
        
    @pytest.mark.slow
    def test_invalid_column_names_with_read_data_with_header(self, spark,sample_data_path):
        schema = StructType().add("name", StringType()).add("age", IntegerType())
        df = read_data(spark, sample_data_path, schema)
        
        assert df.count() == 2
        assert df.schema == schema
        assert df.columns != ["hi", "age"]
        assert df.collect() == [("John", 30), ("Alice", 25)]
        
    @pytest.mark.slow
    def test_invalid_count_with_read_data_with_header(self, spark,sample_data_path):
        schema = StructType().add("name", StringType()).add("age", IntegerType())
        df = read_data(spark, sample_data_path, schema)
        
        assert df.count() != 3
        assert df.schema == schema
        assert df.columns == ["name", "age"]
        assert df.collect() == [("John", 30), ("Alice", 25)]

    @pytest.mark.slow
    def test_invalid_data_with_read_data_with_header(self, spark,sample_data_path):
        schema = StructType().add("name", StringType()).add("age", IntegerType())
        df = read_data(spark, sample_data_path, schema)
        
        assert df.count() == 2
        assert df.schema == schema
        assert df.columns == ["name", "age"]
        assert df.collect() != [("Kahya", 30), ("Alice", 25)]