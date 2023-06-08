import os
import signal
import subprocess
from pyspark.sql import SparkSession
from mock.data import claim, policyholder, provider
from mock.infrastructure.buckets import initialise_for_project
import mock.infrastructure.mock_s3_server as s3_mock_server
from paths.components import Bucket

ENV = Bucket.TEST
s3_mock_server.S3ResourceSingleton.teardown()

def test_row_count_for_all_source_tables_in_landing_tier(glueContext):
    s3_mock_server.S3ResourceSingleton.teardown()
    spark = glueContext.spark_session    
    mock_bucket, process = s3_mock_server.get_mock_s3_server_and_its_local_process(
                                        spark, 
                                        name=str(ENV),
                                        endpoint_url="http://127.0.0.1:5000/")

    try:   
        initialise_for_project(mock_bucket)
        source = mock_bucket.get_source_facts()
        
        for obj in mock_bucket.list_all():
            df = spark.read.csv(f"s3://{obj.bucket_name}/{obj.key}", header=True)
            df.show(5, truncate=True)
            if 'claim-' in obj.key:
                assert df.count() == source["claim"]["count"]
            if 'provider-' in obj.key:
                assert df.count() == source["provider"]["count"]
            if 'policyholder-' in obj.key:
                assert df.count() == source["policyholder"]["count"]
        
    finally:
        mock_bucket.unload_all()
        s3_mock_server.S3ResourceSingleton.teardown()
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)