"""
test_landing_claim.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.landing.claim module.
Execute python3 -m pytest tests/test_landing_claim.py -vvv to run.
"""
import pytz
import etl.jobs.landing.claim
 
from freezegun import freeze_time
from datetime import datetime
from etl.paths.components import Bucket, Source, Tier, Table, Environment, Load
from etl.paths.timestamps import get_current_time_as_timestamp
class TestPaths:
    
    def test_create_claim_landing_to_raw_full_load_get_input_path(self):
        """Test that the create_path function returns the correct write path 
        for claim table data in the landing tier that came from the claim 
        database."""
    
        path = etl.jobs.landing.claim.get_input_path()
        assert path ==  f"{Environment.PROD}://{Bucket.LANDING}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/202305211851-claim-full.csv"
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_landing_to_raw_full_load_output_path(self):
        """Test that the create_path function returns the correct write path 
        for claim table data in the landing tier that came from the claim 
        database."""
    
        path = etl.jobs.landing.claim.get_output_path()
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.RAW}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/{now}/"