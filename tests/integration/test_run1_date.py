import pytest
import stage_date_into_optimised
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions


test_actions = get_print_s3_actions()

@pytest.mark.integration
@infrastructure.with_test_server(test_actions)
def test_run1_date(spark,env):
    
    df = stage_date_into_optimised.run(spark,env)
    assert True