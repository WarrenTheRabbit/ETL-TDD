import os
import subprocess
from pyspark.sql import SparkSession
from mock.infrastructure.mock_s3_bucket import MockS3Bucket
from mock.infrastructure.s3_resource import S3ResourceSingleton

def get_mock_s3_server_and_its_local_process(spark: SparkSession,
                                             name: str,
                                             endpoint_url: str):
    """
    Function to setup and initialize test case execution

    Args:
        spark (SparkSession): PySpark session object

    Returns:
        process: Process object for the moto server that was started
    """
    import inspect
    print(f"Inside {inspect.stack()[0][3]}")
    
    # get the port from the endpoint url
    port = endpoint_url.split(":")[-1].replace("/", "")
    
    
    process = subprocess.Popen(f"moto_server s3 -p{port}",
                            stdout=subprocess.DEVNULL, # Use DEVNULL to suppress.
                            stderr=subprocess.DEVNULL, # Use PIPE to show.
                            shell=True,
                            preexec_fn=os.setsid)

    
    s3 = S3ResourceSingleton.getMockInstance()
    print(s3, "created.")
    print(f"endpoint_url: {s3.meta.client._endpoint}")
    # Get the mock bucket object.
    s3.create_bucket(Bucket=name)

    bucket = s3.Bucket(name)
    # Wrap the bucket with an interface.
    bucket = MockS3Bucket(s3.Bucket(name))
    
    # Configure Spark.
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", endpoint_url)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    return bucket, process
