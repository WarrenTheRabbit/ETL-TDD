from tests.utilities.infrastructure import with_test_server
from tests.utilities.actions import get_print_bucket_contents_actions, \
                                    TestActions

test_actions:TestActions = get_print_bucket_contents_actions()

@with_test_server(test_actions)
def test_run1(spark):
    import automation.runs.run1_provider
    automation.runs.run1_provider.run(spark)
    
    import automation.runs.run1_policyholder
    automation.runs.run1_policyholder.run(spark)
    
    import automation.runs.run1_claim
    automation.runs.run1_claim.run(spark)
    
    import automation.runs.run1_date
    automation.runs.run1_date.run(spark)
