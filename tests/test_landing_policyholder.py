"""
test_landing_provider.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.landing.policyholder module.
Execute python3 -m pytest tests/test_landing_provider.py -vvv to run.
"""

import etl.jobs.landing.policyholder

from freezegun import freeze_time
from datetime import datetime
from etl.paths.components import Bucket, Source, Tier, Dimension, Table, \
    Environment, Load
from etl.paths.timestamps import get_current_time_as_timestamp 

class TestPaths:
    
    def test_create_policyholder_landing_to_raw_full_load_input_path(self):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
    
        path = etl.jobs.landing.policyholder.input_path
        
        assert path == "s3://landing-dev-wm/landing/claim_db/policyholder/full/202305221132.csv"
        assert path ==  f"{Environment.PROD}://{Bucket.LANDING}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/202305221132.csv"
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_policyholder_landing_to_raw_full_load_output_path(self):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
    
        path = etl.jobs.landing.policyholder.output_path
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.RAW}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/{now}/"
        
