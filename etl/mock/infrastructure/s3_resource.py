import boto3

class S3ResourceSingleton:
    _instance = None
    
    def __init__(self, is_mock:bool=False, **kwargs):
        """
        TODO: Add docstring.
        """
        # Guarantee that `region_name` is set by defaulting to 'us-east-1'.
        region_name = kwargs.get('region_name','us-east-1') 
        kwargs['region_name'] = region_name 
        print(f"kwargs is {kwargs}")
        
        if is_mock:
            S3ResourceSingleton._instance = boto3.resource("s3",
                                    endpoint_url="http://127.0.0.1:5000/",
                                    aws_access_key_id="FakeKey",
                                    aws_secret_access_key="FakeSecretKey",
                                    aws_session_token="FakeSessionToken",
                                    **kwargs)
        else:
            S3ResourceSingleton._instance = boto3.resource("s3",
                                                  region_name=region_name)
        self = S3ResourceSingleton._instance

    @staticmethod
    def getInstance(**kwargs):
        if S3ResourceSingleton._instance is None:
            S3ResourceSingleton(is_mock=False, **kwargs)
        return S3ResourceSingleton._instance

    @staticmethod
    def getMockInstance(**kwargs):
        if S3ResourceSingleton._instance is None:
            S3ResourceSingleton(is_mock=True, **kwargs)
        return S3ResourceSingleton._instance
    
    @staticmethod
    def teardown():
        S3ResourceSingleton._instance = None