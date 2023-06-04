import pytest
import pytest
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions
import stage_provider_into_raw
import stage_provider_into_access

print_actions = get_print_s3_actions()

@pytest.mark.integration
@infrastructure.with_test_server(print_actions)
def test_run1_provider(spark,env):
    
    df = stage_provider_into_raw.run(spark,env)
    assert True
    df = stage_provider_into_access.run(spark,env)
    assert True

    

    
    


