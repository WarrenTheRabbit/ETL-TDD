from unittest import mock
import pytest
import pytz
from datetime import datetime
import itertools

from freezegun import freeze_time
from etl.paths.components import StringEnum
from etl.paths.validation import get_validity_of_path_request, \
    only_landing_tier_paths_have_extensions, \
    only_one_table_type_per_path, only_nonanalytical_table_paths_have_sources
from etl.paths.components import Environment, Bucket, Source
from etl.paths.components import Table, Fact, Dimension, Load, Tier
from etl.paths.timestamps import get_lexicographically_highest_subdirectory, get_timestamp_of_most_recently_created_file, \
    get_timestamp_for_file
from etl.paths.create import create_path


class TestLexicographicallyHighest:
    pass

# Mock the "get_lexicographically_highest_subdirectory" function used 
# by "create_path"
def mock_get_lexicographically_highest_subdirectory(*args, **kwargs):
    return 'most_recent'

# Mock the "get_time" function used by "create_path"
def mock_get_time(*args, **kwargs):
    return 'most_recent'

@pytest.fixture()
def mock_time_functions(mocker):
    mocker.patch('etl.paths.create.get_lexicographically_highest_subdirectory', 
                mock_get_lexicographically_highest_subdirectory)
    mocker.patch('etl.paths.create.get_timestamp_for_file', mock_get_time)

class TestCreatePath:           
            
    @freeze_time(datetime.now().strftime("%Y%m%d%H%M"))                      
    @pytest.mark.usefixtures('mock_time_functions')
    def test_create_path_for_different_conditions(self):
        now = (datetime
                .now(pytz.timezone('Australia/Sydney'))
                .strftime("%Y%m%d%H%M")
        )
         
        # Condition 1: has source, time = 'recent' and no file_extension.
        assert create_path(
            environment=Environment.DEV,
            bucket=Bucket.LANDING,
            tier=Tier.LANDING,
            source=Source.CLAIM_DB,
            table=Table.CLAIM,
            load=Load.FULL,
            time_requested='recent'
        ) == "file://{Bucket.PROJECT}/etl/landing/claim_db/claim/full/most_recent/"
        
        # Condition 2: no source, time = 'recent' and no file_extension.
        assert create_path(
            environment=Environment.PROD,
            bucket=Bucket.LANDING,
            tier=Tier.LANDING,
            load=Load.FULL,
            time_requested='recent',
            fact=Fact.CLAIM
        ) == "s3://landing-dev-wm/landing/claim_fact/full/most_recent/"
        
        # Condition 3: no source, time = 'recent' and file_extension.
        # SKIPPED - is an Invalid Condition 4
        
        # Condition 4: source, time = 'recent' and file_extension.
        assert create_path(
            environment=Environment.PROD,
            bucket=Bucket.PROJECT,
            tier=Tier.LANDING,
            source=Source.CLAIM_DB,
            table=Table.POLICYHOLDER,
            load=Load.FULL,
            time_requested='recent',
            file_extension='.csv'
        ) == "s3://project-dev-wm/landing/claim_db/policyholder/full/most_recent.csv"
        
        # Condition 5: no source, time = 'now' and not file_extension.
        assert create_path(
            environment=Environment.DEV,
            bucket=Bucket.PROJECT,
            tier=Tier.OPTIMISED,
            load=Load.FULL,
            time_requested='now',
            fact=Fact.CLAIM
        ) == f"file://project-dev-wm/optimised/claim_fact/full/{now}/"
        
        # Condition 6: source, time = 'now' and not file_extension.
        # SKIPPED - is an Invalid Condition 4
        
        # Condition 7: source, time = 'now' and file_extension.
        assert create_path(
            environment=Environment.PROD,
            bucket=Bucket.LANDING,
            tier=Tier.LANDING,
            source=Source.CLAIM_DB,
            table=Table.PROVIDER,
            load=Load.FULL,
            time_requested='now',
            file_extension='.csv'
        ) == f"s3://landing-dev-wm/landing/claim_db/provider/full/{now}.csv"
                                  

class TestPathValidation:
            
    def test_false_when_path_does_not_have_exacty_one_table_type(self):
        """Tests that False is returned when the function 
        `only_one_table_per_path` is provided with more than one table type.
        Test cases are generated programatically.
        """
        # There are three types of table, and we will use `type_exemplars` as 
        # representative tables. Their types, not their values, are what is 
        # important.
        type_exemplars = [Dimension.DATE, Fact.CLAIM, Table.CLAIM]
                
        # We create an abstraction for the set of options available for each 
        # table type (to either provide it or not provide it). 
        # For example, with Dimension.Date the choice set is: (Dimension.Date,
        # ''), where '' represents not providing it.
        choices_representation = [
            (structure, '')  # conceptually: (provided, not provided)
            for structure 
            in type_exemplars
        ]
        
        # We permute all the ways of selecting from the choice sets by taking 
        # their Cartesian product, and keep only those cases where more than 
        # one type is provided.
        test_cases = [
            case 
            for case in list(itertools.product(*choices_representation))
            if len([choice for choice in case if choice  != '']) != 1 ]
        
        # Run test cases by providing the test cases as arguments to the 
        # function undergoing testing.
        for args in test_cases:
            assert only_one_table_type_per_path(*args) == False
        
    def test_true_when_path_has_exactly_one_table_type(self):
        """Tests that True is returned when passed exactly one structure."""
        # Generate test cases.
        type_exemplars = [Dimension.DATE, Fact.CLAIM, Table.CLAIM]
        test_cases = [
            [type_exemplars[i] if i ==j else '' for j in range(3)]
            for i 
            in range(3)
        ]
        # Run test cases by providing the test cases as arguments to the 
        # function undergoing testing.
        for args in test_cases:
            assert only_one_table_type_per_path(*args) == True
        
        
    def test_false_when_path_is_for_nonlanding_tier_and_has_file_extension(self):
        """Tests that False is returned when a path to a tier that is not a 
        landing tier is provided with a file extension."""
        tiers = [Tier.RAW, Tier.ACCESS, Tier.OPTIMISED]
        extension = '.csv'
        for tier in tiers:
            args = (tier, extension)
            assert only_landing_tier_paths_have_extensions(*args) == False
    
    def test_true_when_path_is_for_landing_tier_and_has_file_extension(self):
        """Tests that True is returned when a path to a tier that is a 
        landing tier is provided with a file extension."""
        tier = Tier.LANDING
        extension = '.csv'
        args = (tier, extension)
        assert only_landing_tier_paths_have_extensions(*args) == True
        
    def test_true_when_path_has_no_file_extension(self):
        """Tests that True is returned when no file extension is provided 
        (regardless of tier)."""
        tiers = [Tier.LANDING, Tier.RAW, Tier.ACCESS, Tier.OPTIMISED]
        extension = ''
        for tier in tiers:
            args = (tier, extension)
            assert only_landing_tier_paths_have_extensions(*args) == True
            
class TestGetLexicographicallyHighestSubdirectory:
    
    def test_returns_correct_subdirectory_from_test_bucket(self):
        """Tests that the correct subdirectory is returned."""
        # setup
        bucket = 'test-dev-wm'
        path = 'raw/claim_db/claim/full/'
        
        result = get_lexicographically_highest_subdirectory(bucket,path)
        expected = '202305211852'
        
        assert result == expected