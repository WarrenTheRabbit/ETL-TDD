# `test_mock_s3.py`

### When run individually, it passes.
`STDOUT`:
kwargs is {'region_name': 'us-east-1'}
s3.ServiceResource()
endpoint_url: s3(http://127.0.0.1:5000/)
bucket: <etl.mock.infrastructure.mock_s3_bucket.MockS3Bucket object at 0x7f8d509f8910>
+-------+----------+
|   name|        dt|
+-------+----------+
|    sam|1962-05-25|
|    let|1999-05-21|
|   nick|1996-04-03|
| person|1996-04-03|
|personb|1996-03-02|
+-------+----------+

s3.ServiceResource()

## tests/mock_ojects/test_mock_landing_data.py::test_row_count_for_all_source_tables_in_landing_tier FAILED  