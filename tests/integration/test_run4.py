import pytest
import stage_claim_into_optimised
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions

# Prints content of s3 Bucket at setup, assert and teardown.
print_actions = get_print_s3_actions()

@pytest.mark.skip(reason="Dependent files are torndown by previous runs.")
@pytest.mark.integration
@infrastructure.with_test_server(print_actions)
def test_run_4(spark, env):

    # Run 4 starts here.
    df = stage_claim_into_optimised.run(spark, env)
    assert True