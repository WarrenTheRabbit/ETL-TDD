import json
import boto3

sns = boto3.client('sns')
s3 = boto3.client('s3')
s3Resource = boto3.resource('s3')


def get_profile(s3,bucket, key):
    profileObject = s3.get_object(Bucket = bucket, 
                                  Key = key)
    profileContent = json.loads(profileObject['Body'].read().decode('utf-8'))
    
    return profileContent



profileOutputBucket = 'project-lf'
profileOutputPrefix = 'data_quality/claim_db/claim/202305211851_07332d8ca92c65d733d9511f992015e8579d4105116596568f6e10e19f74fe47.json'   
s3 = boto3.client('s3', 'us-east-1')


profile = get_profile(s3, profileOutputBucket, profileOutputPrefix)
print(profile['columns'])

