"""
test_access_provider.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.access.provider module.
Execute python3 -m pytest tests/test_access_provider.py -vvv to run.
"""
from datetime import datetime
from freezegun import freeze_time
from etl.paths.create import create_path
from etl.paths.components import Bucket, Source, Tier, Dimension, Table, Environment, Load
from etl.jobs.access.provider import provider_table_input_path, provider_dimension_output_path

class TestPaths:
    
    def test_create_claim_access_to_optimised_full_load_input_path(self):
        """"""        
        path = provider_table_input_path
        expected_path = create_path(
            environment=Environment.PROD,
            bucket=Bucket.PROJECT,
            tier=Tier.ACCESS,
            source=Source.CLAIM_DB,
            table=Table.PROVIDER,
            load=Load.FULL,
            time_requested="recent"
        )
        assert path == expected_path 
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_access_to_optimised_full_load_output_path(self):
        """"""
        path = provider_dimension_output_path    
        expected_path = create_path(
            environment=Environment.PROD,
            bucket=Bucket.PROJECT,
            tier=Tier.OPTIMISED,
            dimension=Dimension.PROVIDER,
            load=Load.FULL,
            time_requested="now"
        )     
        assert path ==  expected_path
        
