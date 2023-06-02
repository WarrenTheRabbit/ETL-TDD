import os
import signal
import subprocess
from pyspark.sql import SparkSession
from etl.paths.components import Bucket
from etl.mock.infrastructure import s3_bucket
from etl.mock.infrastructure.buckets import initialise_for_project


def test_first_provider_workflow(glueContext):
    
    spark = glueContext.spark_session
    mock_bucket, process = s3_bucket.get_mock_s3_server_and_its_local_process(
                                        spark, 
                                        name=Bucket.PROJECT.value,
                                        endpoint_url="http://127.0.0.1:5000/")   
    
    initialise_for_project(mock_bucket)
    for obj in mock_bucket.list_all():
        print(obj)
    
    try:   
        import workflow_1
        workflow_1.run(spark)
        
        print("The following objects were created during the test:")
        for obj in mock_bucket.list_all():
            print(f"s3://{obj.bucket_name}/{obj.key}")  
    finally:
        mock_bucket.unload_all()
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)