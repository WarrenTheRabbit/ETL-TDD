"""
test_landing_provider.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.landing.provider module.
Execute python3 -m pytest tests/test_landing_provider.py -vvv to run.
"""
from freezegun import freeze_time
from datetime import datetime
from etl.paths.create import create_path
from etl.paths.components import Bucket, Source, Tier, Dimension, Table, \
    Environment, Load
from etl.paths.timestamps import get_current_time_as_timestamp 
import etl.jobs.landing.provider 

class TestPaths:
    
    def test_create_claim_landing_to_raw_full_load_input_path(self):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
    
        path = etl.jobs.landing.provider.input_path
        
        assert path ==  f"{Environment.PROD}://{Bucket.LANDING}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/202305221136.csv"
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_landing_to_raw_full_load_output_path(self):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
    
        path = etl.jobs.landing.provider.output_path
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.RAW}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/{now}/"
        
