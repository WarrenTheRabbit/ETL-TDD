"""
test_optimised_location_dim.py
~~~~~~~~~~~~~~~~~~~~~
This module contains unit tests for the etl.jobs.optimised.location_dim module.
Execute python3 -m pytest tests/test_optimised_location_dim.py -vvv to run
in verbose mode.
"""
from datetime import datetime
from freezegun import freeze_time

class TestPaths:
    
    def test_create_provider_input_path(self):    
        pass
    
    def test_create_policyholder_input_path(self):
        pass
    
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_location_dim_output_path(self):
        pass
 