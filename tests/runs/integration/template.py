import os
import signal
import subprocess
from etl.paths.components import Bucket
from etl.mock.infrastructure import s3_bucket
from etl.mock.infrastructure.buckets import initialise_for_project


def test_run1(glueContext):
    
    spark = glueContext.spark_session
    mock_bucket, process = s3_bucket.get_mock_s3_server_and_its_local_process(
                                        spark, 
                                        name=Bucket.PROJECT.value,
                                        endpoint_url="http://127.0.0.1:5000/")   
    
    initialise_for_project(mock_bucket)

    for obj in mock_bucket.list_all():
        print(obj)
    
    try:   
        print("The following objects existed before testing the modules:")
        import run1_provider
        run1_provider.run(spark)
        
        import run1_policyholder
        run1_policyholder.run(spark)
        
        import run1_claim
        run1_claim.run(spark)
        
        print("The following objects were created during the test:")
        for obj in mock_bucket.list_all():
            print(f"s3://{obj.bucket_name}/{obj.key}")  
    finally:
        mock_bucket.unload_all()
        print("The following objects exist after the test:")
        for obj in mock_bucket.list_all():
            print(f"s3://{obj.bucket_name}/{obj.key}")  
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)