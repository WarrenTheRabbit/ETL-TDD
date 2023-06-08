import os
import boto3
import signal
import subprocess
import pytest
from etl.jobs.landing.claim import read_data, transform_data, write_data
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from mock.infrastructure.mock_s3_server import S3ResourceSingleton, \
    get_mock_s3_server_and_its_local_process
from paths.components import Bucket

SOURCE_NAME = "data.csv"
TABLE_NAME = "dummy"
BUCKET = Bucket.MOCK
ENDPOINT_URL = "http://127.0.0.1:5000/"
S3ResourceSingleton.teardown()


@pytest.mark.slow
def test_server():
    if S3ResourceSingleton._instance != None:
        S3ResourceSingleton.teardown() 
    
    spark = SparkSession.builder.appName("test").getOrCreate()
    mock_bucket, process = get_mock_s3_server_and_its_local_process(
                            spark,
                            name = str(BUCKET),
                            endpoint_url = ENDPOINT_URL)
    print(f"bucket: {mock_bucket.bucket.name}")
    data = 'name,dt\nsam,1962-05-25\nlet,1999-05-21\nnick,1996-04-03\nperson,1996-04-03\npersonb,1996-03-02'
    mock_bucket.load(Key="data.csv", Body=data)
    try: 
        df = spark.read.csv(f"s3://{str(BUCKET)}/data.csv", header=True)
        df.show()
        assert df.count() == 5
        assert df.columns == ['name', 'dt']
        spark.stop()
    finally:
        print("Finally....")
        if S3ResourceSingleton._instance != None:
             print("{S3ResourceSingleton._instance} exists. Deleting...")
             S3ResourceSingleton.teardown()
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)