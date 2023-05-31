    
import pytest
import boto3
import pytest
import json
from moto import mock_s3
from etl.dataquality.claim_db.landing.claim.validate import get_profile

class TestGetProfile:

    def test_get_profile(self):
        
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_resource = boto3.resource('s3', region_name='us-west-2')
        
        # Create a mock bucket
        bucket = 'my-test-bucket'
        key = 'my-test-key.json'
        
        s3_resource.create_bucket(Bucket=bucket)
        profile_content = {
            'name': 'John Doe',
            'email': 'john@example.com'
        }
        
        s3_resource.put_object(Bucket=bucket, Key=key, Body=json.dumps(profile_content))

        # Call your function
        result = get_profile(s3_client,bucket, key)

        # Verify the results
        assert result == profile_content
