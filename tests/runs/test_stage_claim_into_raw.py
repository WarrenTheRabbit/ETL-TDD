from tests.utilities import infrastructure
from tests.utilities.actions import TestActions
from tests.utilities.actions import get_print_bucket_contents_actions

# A TestActions object has three attributes: pre_test, during_test, and post_test.
test_actions:TestActions = get_print_bucket_contents_actions()

@infrastructure.with_test_server(test_actions)
def test_staging_claim_data_from_landing_into_raw(spark): 
    import stage_claim_into_raw
    df = stage_claim_into_raw.run(spark)
    assert df.count() == 300