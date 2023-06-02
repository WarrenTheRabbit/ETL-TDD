from etl.mock.infrastructure.s3_bucket import MockS3Bucket

def initialise_for_project(mock_bucket: MockS3Bucket):
    mock_bucket.unload_all()
    if mock_bucket.list_all() != []:
        raise Exception("Mock bucket is NOT empty after `unload_all`.")
    mock_bucket.load_with_source_data()
    if mock_bucket.list_all() == []:
        raise Exception("Mock bucket IS empty after load.")
    
    