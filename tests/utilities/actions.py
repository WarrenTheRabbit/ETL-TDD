from tests.utilities.presentation import print_objects_in_bucket

class Actions:
    def __init__(self, pre_test, during_test, post_test):
        self.pre_test = pre_test
        self.during_test = during_test
        self.post_test = post_test


def get_print_bucket_contents_actions():
    return Actions(
        pre_test=lambda mock_bucket: print_objects_in_bucket("existed before running the test.", mock_bucket),
        during_test=lambda mock_bucket: print_objects_in_bucket("existed during the test.", mock_bucket),
        post_test=lambda mock_bucket: print_objects_in_bucket("exist after the test.", mock_bucket),
    )
