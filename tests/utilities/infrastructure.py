import colorama
import os
import pytest
import signal

from colorama import Fore, Style
from mock.infrastructure import mock_s3_server
from mock.infrastructure.s3_resource import S3ResourceSingleton
import mock.infrastructure.buckets as buckets
from paths.components import Bucket
from tests.utilities.actions import TestActions
from paths.components import Bucket

ENV = str(Bucket.MOCK)


def with_test_server(actions:TestActions):
    def inner_decorator(test_function):
        def wrapped_test(glueContext, s3_resource):
            # As a defensive measure, start with S3 resource teardown.
            S3ResourceSingleton.teardown()
            
            # Setup all project resources.
            spark = glueContext.spark_session
            mock_bucket, process = mock_s3_server.get_mock_s3_server_and_its_local_process(
                                                spark,
                                                name=ENV,
                                                endpoint_url="http://127.0.0.1:5000/")   
            buckets.initialise_for_project(mock_bucket)

            try:
                # Conduct the test.
                actions.pre_test(mock_bucket)
                test_function(spark, env=ENV)
                actions.during_test(mock_bucket)   
            finally:
                # Teardown all resources.
                mock_bucket.unload_all()
                actions.post_test(mock_bucket)
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                S3ResourceSingleton.teardown()
        
        return wrapped_test
    return inner_decorator



    ...
