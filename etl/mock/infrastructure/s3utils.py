import boto3

class S3Resource:
    _instance = None

    def __init__(self, s3=None):
        if S3Resource._instance is not None:
            raise Exception("This class is a singleton!")
        else:
            if s3 is None:
                self._s3 = boto3.resource('s3')
            else:
                self._s3 = s3
            S3Resource._instance = self

    @staticmethod
    def getInstance():
        if S3Resource._instance is None:
            S3Resource()
        return S3Resource._instance

    def get_resource(self):
        return self._s3
