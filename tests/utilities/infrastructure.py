import colorama
import os
import signal

from colorama import Fore, Style
from etl.mock.infrastructure import s3_bucket
from etl.mock.infrastructure import buckets as test_infrastructure
from etl.paths.components import Bucket
from tests.utilities.actions import Actions

def with_test_server(actions:Actions):
    def inner_decorator(test_function):
        def wrapped_test(glueContext):
            spark = glueContext.spark_session
            mock_bucket, process = s3_bucket.get_mock_s3_server_and_its_local_process(
                                                spark, 
                                                name=Bucket.PROJECT.value,
                                                endpoint_url="http://127.0.0.1:5000/")   
            test_infrastructure.initialise_for_project(mock_bucket)

            try:
                actions.pre_test(mock_bucket)
                test_function(spark)
                actions.during_test(mock_bucket)   
            finally:
                mock_bucket.unload_all()
                actions.post_test(mock_bucket)
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        
        return wrapped_test
    return inner_decorator


