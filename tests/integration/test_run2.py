import pytest
import stage_procedure_into_optimised
import stage_location_into_optimised
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions

print_actions = get_print_s3_actions()

@pytest.mark.skip(reason="Dependent files are torndown by previous runs.")
@pytest.mark.integration
@infrastructure.with_test_server(print_actions)
def test_run_2(spark, env):
    
    # Run 2 starts here.
    df = stage_location_into_optimised.run(spark, env)
    assert True

    df = stage_procedure_into_optimised.run(spark, env)
    assert True