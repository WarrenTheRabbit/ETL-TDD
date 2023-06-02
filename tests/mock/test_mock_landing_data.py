import os
import signal
import subprocess
from pyspark.sql import SparkSession
from etl.mock.data import claim, policyholder, provider
from etl.mock.infrastructure.buckets import initialise_for_project
import etl.mock.infrastructure.s3_bucket as s3_bucket


def test_row_count_for_all_source_tables_in_landing_tier(glueContext):
       
    spark = glueContext.spark_session
    
    # A reference to the bucket resource is used for object-oriented 
    # interactions, such as `put_object`. A reference to the process is used 
    # to stop the server - this makes tests repeatable and independent.
    mock_bucket, process = s3_bucket.get_mock_s3_server_and_its_local_process(
                                        spark, 
                                        name="project-lf",
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
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)