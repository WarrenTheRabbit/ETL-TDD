import os
import boto3
import subprocess
from pyspark.sql import SparkSession
from etl.mock.data import claim, policyholder, provider
import etl.mock.infrastructure.s3utils as s3utils


class MockS3Bucket:
    
    def __init__(self, bucket):
        self.bucket = bucket
    
    @property
    def name(self):
        return self.bucket.name
        
    def load(self, Body, Key):
        self.bucket.put_object(Body=Body, Key=Key)
        
    def unload_all(self):
        self.bucket.objects.all().delete()
        
    def list_all(self):
        return list(self.bucket.objects.all())
    
    def get_source_facts(self):
        return  {
            "claim": {
                "data": claim.landing_csv, 
                "key": "etl/landing/claim_db/claim/full/202305211851-claim-full.csv",
                "count": 300
            },
            "policyholder": {
                "data": policyholder.landing_csv, 
                "key": "etl/landing/claim_db/policyholder/full/202305211851-policyholder-full.csv",
                "count": 30
            },  
            "provider": {
                "data": provider.landing_csv, 
                "key": "etl/landing/claim_db/provider/full/202305211851-provider-full.csv",
                "count": 5
            }
        }
        
    def load_with_source_data(self):
        sources_to_load = self.get_source_facts()
            
        for table in sources_to_load.keys():
            data = sources_to_load[table]["data"]
            key = sources_to_load[table]["key"]
            self.bucket.put_object(Body=data, Key=key)  

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
    # get the port from the endpoint url
    port = endpoint_url.split(":")[-1].replace("/", "")
    
    process = subprocess.Popen(
        f"moto_server s3 -p{port}",
        stdout=subprocess.PIPE,
        shell=True,
        preexec_fn=os.setsid,
    )

    s3 = boto3.resource(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="FakeKey",
        aws_secret_access_key="FakeSecretKey",
        aws_session_token="FakeSessionToken",
        region_name="us-east-1"
    )
    
    s3 = s3utils.S3Resource(s3).get_resource()
    
    s3.create_bucket(Bucket=name)
    

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", endpoint_url)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  
    # Get the mock bucket object
    bucket = MockS3Bucket(s3.Bucket(name))

    return bucket, process


            