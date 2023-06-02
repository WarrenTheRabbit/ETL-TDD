import etl.jobs.landing.claim
import etl.jobs.landing.provider
import etl.jobs.landing.policyholder
import etl.jobs.raw.claim
 
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
        
        
    def test_create_provider_landing_to_raw_full_load_get_input_path(self):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
    
        path = etl.jobs.landing.provider.get_input_path()
        
        assert path ==  f"{Environment.PROD}://{Bucket.LANDING}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/202305221136-provider-full.csv"
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_provider_landing_to_raw_full_load_output_path(self):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
    
        path = etl.jobs.landing.provider.get_output_path()
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.RAW}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/{now}/"
        
    def test_create_policyholder_landing_to_raw_full_load_get_input_path(self):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
    
        path = etl.jobs.landing.policyholder.get_input_path()
        
        assert path == "s3://landing-dev-wm/landing/claim_db/policyholder/full/202305221132-policyholder-full.csv"
        assert path ==  f"{Environment.PROD}://{Bucket.LANDING}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/202305221132.csv"
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_policyholder_landing_to_raw_full_load_output_path(self):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
    
        path = etl.jobs.landing.policyholder.get_output_path()
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.RAW}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/{now}/"
        
     def test_create_claim_raw_to_access_full_load_get_input_path(self):
        """Test that the input path is correct for reading the claim_db.claim 
        data that has been staged in the Raw tier."""

        path = etl.jobs.raw.claim.get_input_path()  
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
    
        path = etl.jobs.raw.claim.get_output_path()
        now = get_current_time_as_timestamp()
        assert path ==  f"{Environment.PROD}://{Bucket.ACCESS}/{Tier.ACCESS}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/{now}/"
    
    # def test_create_claim_access_to_optimised_full_load_get_input_path(self):
    #     """"""        
    #     path = get_input_path()
    #     expected_path = create_path(
    #         environment=Environment.PROD,
    #         bucket=Bucket.PROJECT,
    #         tier=Tier.ACCESS,
    #         source=Source.CLAIM_DB,
    #         table=Table.PROVIDER,
    #         load=Load.FULL,
    #         time_requested="recent"
    #     )
    #     assert path == expected_path 
        
    # @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    # def test_create_claim_access_to_optimised_full_load_output_path(self):
    #     """"""
    #     path = get_provider_output_path()    
    #     expected_path = create_path(
    #         environment=Environment.PROD,
    #         bucket=Bucket.PROJECT,
    #         tier=Tier.OPTIMISED,
    #         dimension=Dimension.PROVIDER,
    #         load=Load.FULL,
    #         time_requested="now"
    #     )     
    #     assert path ==  expected_path