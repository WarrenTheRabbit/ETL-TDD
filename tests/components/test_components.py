import pytest
import pytz
from freezegun import freeze_time
from datetime import datetime
from mock.infrastructure.s3_resource import S3ResourceSingleton

from paths.components import Environment, Bucket, Source, StringEnum
from paths.components import Table, Fact, Dimension, Load, Tier
from paths.timestamps import get_timestamp_for_file, \
    get_timestamp_of_most_recently_created_file


class TestBucketEnum:
    def test_enum_values(self):
        Bucket.PROD.value == "project-lf"
        Bucket.TEST.value == "test-lf-wm"
        Bucket.MOCK.value == "not-real"

    def test_enum_type(self):
        pass

class TestSourceEnum:
    def test_enum_values(self):
        assert str(Source.CLAIM_DB) == "claim_db"

    def test_enum_type(self):
        assert isinstance(Source.CLAIM_DB, StringEnum)

class TestEnvironmentEnum:
    def test_enum_values(self):
        assert str(Environment.AWS) == "s3"
        # assert str(Environment.DEV) == "file"

    def test_enum_type(self):
        assert isinstance(Environment.AWS, StringEnum)
        
class TestTableEnum:
    def test_enum_values(self):
        assert str(Table.CLAIM) == "claim"
        assert str(Table.PROVIDER) == "provider"
        assert str(Table.POLICYHOLDER) == "policyholder"

    def test_enum_type(self):
        assert isinstance(Table.CLAIM, StringEnum)

class TestFactEnum:
    def test_enum_values(self):
        assert str(Fact.CLAIM) == "claim_fact"

    def test_enum_type(self):
        assert isinstance(Fact.CLAIM, StringEnum)

class TestDimensionEnum:
    def test_enum_values(self):
        assert str(Dimension.DATE) == 'date_dim'
        assert str(Dimension.LOCATION) == 'location_dim'
        assert str(Dimension.POLICYHOLDER) == 'policyholder_dim'
        assert str(Dimension.PROCEDURE) == 'procedure_dim'
        assert str(Dimension.PROVIDER) == 'provider_dim'

    def test_enum_type(self):
        for dimension in Dimension:
            assert isinstance(dimension, StringEnum)

class TestTierEnum:
    def test_enum_values(self):
        assert str(Tier.LANDING) == 'etl/landing'
        assert str(Tier.RAW) == 'etl/raw'      
        assert str(Tier.ACCESS) == 'etl/access'
        assert str(Tier.OPTIMISED) == 'etl/optimised'
        
    def test_enum_type(self):
        for dimension in Dimension:
            assert isinstance(dimension, StringEnum)


class TestLoadEnum:
    def test_enum_values(self):
        assert str(Load.FULL) == 'full'
        assert str(Load.INCREMENTAL) == 'incremental'

    def test_enum_type(self):
        for load in Load:
            assert isinstance(load, StringEnum)
            
class TestTime:
    
    def test_get_recent_time(self):
        S3ResourceSingleton.teardown()
        try:
            path = 's3://test-dev-wm/landing/claim_db/claim/full/'
            time = get_timestamp_of_most_recently_created_file(path)
            
            assert time == '202306211851'
        finally:
            S3ResourceSingleton.teardown()
            
    def test_get_time_with_recent_flag(self):
        S3ResourceSingleton.teardown()
        try:
            path = 's3://test-dev-wm/landing/claim_db/claim/full/'
            time = get_timestamp_for_file(time_requested='recent', path=path)

            assert time == '202306211851'
        finally:
            S3ResourceSingleton.teardown()
            
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))
    def test_get_time_with_now_flag(self):
        path = 's3://test-dev-wm/landing/claim_db/claim/full/'
        time = get_timestamp_for_file(time_requested='now', path=path)     
        assert time == datetime.now(pytz.timezone('Australia/Sydney')).strftime("%Y%m%d%H%M")
        
    def test_get_time_with_invalid_keyword_flag(self):
        path = 's3://test-dev-wm/landing/claim_db/claim/full/'
        with pytest.raises(ValueError):
            get_timestamp_for_file(time_requested='invalid', path=path)
            