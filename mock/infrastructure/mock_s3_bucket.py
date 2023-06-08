import os
import boto3
import subprocess
from pyspark.sql import SparkSession
from mock.data import claim, policyholder, provider
import mock.infrastructure.s3_resource as s3_resource


class MockS3Bucket:
    
    def __init__(self, bucket):
        self.bucket = bucket
    
    @property
    def name(self):
        return self.bucket.name
        
    def load(self, Body, Key):
        self.bucket.put_object(Body=Body, Key=Key)
        
    def unload_all(self):
        print("unload_all()")
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



            