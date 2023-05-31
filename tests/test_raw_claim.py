"""
test_raw_claim.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.raw.claim module.
Execute python3 -m pytest tests/test_raw_claim.py -vvv to run.
"""
from freezegun import freeze_time
from datetime import datetime
from etl.paths.create import create_path
from etl.paths.components import Bucket, Source, Tier, Dimension, Table, \
    Environment, Load
from etl.paths.timestamps import get_current_time_as_timestamp
import etl.jobs.raw.claim 


class TestPaths:
           
    def test_create_claim_raw_to_access_full_load_input_path(self):
        """Test that the input path is correct for reading the claim_db.claim 
        data that has been staged in the Raw tier."""

        path = etl.jobs.raw.claim.input_path  
        expected_path = create_path(
            environment=Environment.PROD,
            bucket=Bucket.PROJECT,
            tier=Tier.RAW,
            source=Source.CLAIM_DB,
            table=Table.CLAIM,
            load=Load.FULL,
            time_requested="recent"
        )
        assert path == expected_path
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_raw_to_access_full_load_output_path(self):
        """Test that the output path is correct for staging the claim_db.claim
        data in Access tier."""
    
        path = etl.jobs.raw.claim.output_path
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.ACCESS}/{Tier.ACCESS}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/{now}/"
        
        