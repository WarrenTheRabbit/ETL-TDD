import pytest
import stage_provider_into_optimised
import stage_policyholder_into_optimised
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions

# Prints content of s3 Bucket at setup, assert and teardown.
print_actions = get_print_s3_actions()

@pytest.mark.skip(reason="Dependent files are torndown by previous runs.")
@pytest.mark.integration
@infrastructure.with_test_server(print_actions)
def test_run_3(spark, env):
    
    # Run 3 starts here.
    df = stage_policyholder_into_optimised.run(spark, env)
    assert True

    df = stage_provider_into_optimised.run(spark, env)
    assert True