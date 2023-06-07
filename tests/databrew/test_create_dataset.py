import pytest
import boto3
from moto import mock_databrew
from automation.databrew import DataBrew  # replace this with your actual module name

@mock_databrew
def test_create_dataset(mocker):
    databrew = DataBrew(region_name='us-west-2')
    
    # Mock the create_dataset method in the client
    mocker.patch.object(databrew.client, 'create_dataset', return_value={
        'ResponseMetadata': {
            'RequestId': '12345',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'date': 'Fri, 04 Jun 2023 00:00:00 GMT',
                'content-type': 'application/json',
                'content-length': '123',
                'connection': 'keep-alive'
            },
            'RetryAttempts': 0
        },
        'Name': 'test-dataset',
        'Input': {
            'DataCatalogInputDefinition': {
                'CatalogId': '618572314333',
                'DatabaseName': 'test-release-3',
                'TableName': 'test-table',
            }
        },
        'CreatedBy': 'test',
        'CreateDate': '2023-06-04T00:00:00Z',
        'LastModifiedBy': 'test',
        'LastModifiedDate': '2023-06-04T00:00:00Z'
    })
    
    response = databrew.create_dataset('test-dataset', 'test-table')
    
    # Assert that the response is correct
    assert response['Name'] == 'test-dataset'
    assert response['Input']['DataCatalogInputDefinition']['TableName'] == 'test-table'


def test_create_dataset_on_aws():
    databrew = DataBrew('us-east-1')
    response = databrew.create_dataset('test-dataset', 'test-table')