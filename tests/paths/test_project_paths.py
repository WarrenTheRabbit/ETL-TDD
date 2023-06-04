import etl.jobs.landing.claim
import etl.jobs.landing.provider
import etl.jobs.landing.policyholder
import etl.jobs.raw.claim
from etl.mock.infrastructure.mock_s3_server import S3ResourceSingleton
 
from freezegun import freeze_time
from datetime import datetime
from etl.paths.components import Bucket, Source, Tier, Table, Environment, Load
from etl.paths.timestamps import get_current_time_as_timestamp
from etl.paths.create import create_path
ENV = Bucket.TEST
S3ResourceSingleton.teardown()


class TestPaths:
    
    
    def test_create_claim_landing_to_raw_full_load_get_input_path(self, s3_resource):
        """Test that the create_path function returns the correct write path 
        for claim table data in the landing tier that came from the claim 
        database."""
        S3ResourceSingleton.teardown()
        try:
          path = etl.jobs.landing.claim.get_input_path(ENV)
          assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/202305211851-claim-full.csv"
        finally:
            S3ResourceSingleton.teardown()

               
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_landing_to_raw_full_load_output_path(self,s3_resource):
        """Test that the create_path function returns the correct write path 
        for claim table data in the landing tier that came from the claim 
        database."""
        S3ResourceSingleton.teardown()
        try:    
            path = etl.jobs.landing.claim.get_output_path(ENV)
            now = get_current_time_as_timestamp()
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/{now}/"
        finally:
            S3ResourceSingleton.teardown()

        
    def test_create_provider_landing_to_raw_full_load_get_input_path(self,s3_resource):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
        S3ResourceSingleton.teardown()
        try:
            path = etl.jobs.landing.provider.get_input_path(ENV)
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/202305221136-provider-full.csv"
        finally:
            S3ResourceSingleton.teardown()

    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_provider_landing_to_raw_full_load_output_path(self,s3_resource):
        """Test that the create_path function returns the correct write path 
        for provider table data in the landing tier that came from the provider 
        database."""
        S3ResourceSingleton.teardown()
        try:
            path = etl.jobs.landing.provider.get_output_path(ENV)
            now = get_current_time_as_timestamp()
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.PROVIDER}/{Load.FULL}/{now}/"
        finally:
            S3ResourceSingleton.teardown()

    def test_create_policyholder_landing_to_raw_full_load_get_input_path(self,s3_resource):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
        S3ResourceSingleton.teardown()
        print("Value of s3 resource:",S3ResourceSingleton._instance)
        try:
            path = etl.jobs.landing.policyholder.get_input_path(ENV)

            assert path == f"s3://{Bucket.TEST}/etl/landing/claim_db/policyholder/full/202305221132-policyholder-full.csv"
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.LANDING}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/202305221132-policyholder-full.csv"
        finally:
            S3ResourceSingleton.teardown()
        
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_policyholder_landing_to_raw_full_load_output_path(self,s3_resource):
        """Test that the create_path function returns the correct write path 
        for policyholder table data in the landing tier that came from the policyholder 
        database."""
        S3ResourceSingleton.teardown()
        try:
            path = etl.jobs.landing.policyholder.get_output_path(ENV)
            now = get_current_time_as_timestamp()
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.RAW}/{Source.CLAIM_DB}/{Table.POLICYHOLDER}/{Load.FULL}/{now}/"
        finally:
            S3ResourceSingleton.teardown()

        
    def test_create_claim_raw_to_access_full_load_get_input_path(self,s3_resource):
        """Test that the input path is correct for reading the claim_db.claim 
        data that has been staged in the Raw tier."""
        S3ResourceSingleton.teardown()
        try:
            path = etl.jobs.raw.claim.get_input_path(ENV)  
            expected_path = create_path(environment=Environment.AWS,
                                        bucket=ENV,
                                        tier=Tier.RAW,
                                        source=Source.CLAIM_DB,
                                        table=Table.CLAIM,
                                        load=Load.FULL,
                                        time_requested="recent")
            assert path == expected_path
        finally:
            S3ResourceSingleton.teardown()

    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_create_claim_raw_to_access_full_load_output_path(self,s3_resource):
        """Test that the output path is correct for staging the claim_db.claim
        data in Access tier."""
        S3ResourceSingleton.teardown()
        try:
            path = etl.jobs.raw.claim.get_output_path(ENV)
            now = get_current_time_as_timestamp()
            assert path ==  f"{Environment.AWS}://{Bucket.TEST}/{Tier.ACCESS}/{Source.CLAIM_DB}/{Table.CLAIM}/{Load.FULL}/{now}/"
        finally:
            S3ResourceSingleton.teardown()

