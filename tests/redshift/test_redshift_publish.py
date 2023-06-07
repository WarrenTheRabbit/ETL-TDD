import pytest
from unittest.mock import MagicMock, Mock
from automation.glue import Glue
from automation.sns import SNS
from automation.redshift import Redshift

@pytest.fixture
def mock_glue():
    mock = Mock(spec=Glue)
    mock.create_crawler_path.return_value = 's3://bucket/etl/optimised/table/full/2023'
    return mock

@pytest.fixture
def mock_sns():
    return Mock(spec=SNS)

@pytest.fixture
def redshift(mock_glue, mock_sns):
    Redshift.glue = mock_glue
    Redshift.sns = mock_sns
    return Redshift()

def test_get_all_redshift_loads(redshift):
    paths = ['s3://bucket/etl/optimised/table/full/2023', 's3://bucket/etl/raw/claim_db/table/full/2023']
    expected_paths = ['s3://bucket/etl/optimised/table/full/2023']
    
    result = redshift.get_all_redshift_loads(paths)
    
    assert result == expected_paths

def test_create_copy_statement(redshift):
    path = 's3://bucket/etl/optimised/table/full/2022'
    expected_statement = """COPY table
    FROM 's3://bucket/etl/optimised/table/full/2022'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""
    
    result = redshift.create_copy_statement(path)
    
    assert result == expected_statement

def test_get_copy_commands(redshift):
    paths = ['s3://bucket/etl/optimised/table/full/1111', 's3://bucket/etl/raw/table/full/1111']
    expected_commands = ["""COPY table
    FROM 's3://bucket/etl/optimised/table/full/1111'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""]
    
    result = redshift.get_copy_commands(paths)
    
    assert result == expected_commands
