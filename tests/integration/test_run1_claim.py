import pytest
import stage_claim_into_access
import stage_claim_into_raw
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions


test_actions = get_print_s3_actions()

@pytest.mark.integration
@infrastructure.with_test_server(test_actions)
def test_run1_claim(spark, env):
    
    df = stage_claim_into_raw.run(spark, env)
    assert True

    df = stage_claim_into_access.run(spark, env)
    assert True
    
