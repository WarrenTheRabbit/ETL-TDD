import colorama
import os
import pytest
import signal

from colorama import Fore, Style
from etl.mock.infrastructure import mock_s3_server
from etl.mock.infrastructure.s3_resource import S3ResourceSingleton
import etl.mock.infrastructure.buckets as buckets
from etl.paths.components import Bucket
from tests.utilities.actions import TestActions
from etl.paths.components import Bucket

ENV = Bucket.MOCK


def with_test_server(actions:TestActions):
    def inner_decorator(test_function):
        def wrapped_test(glueContext, s3_resource):
            spark = glueContext.spark_session
            S3ResourceSingleton.teardown()
            mock_bucket, process = mock_s3_server.get_mock_s3_server_and_its_local_process(
                                                spark,
                                                name=str(ENV),
                                                endpoint_url="http://127.0.0.1:5000/")   
            buckets.initialise_for_project(mock_bucket)

            try:
                actions.pre_test(mock_bucket)
                test_function(spark, env=ENV)
                actions.during_test(mock_bucket)   
            finally:
                mock_bucket.unload_all()
                actions.post_test(mock_bucket)
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        
        return wrapped_test
    return inner_decorator



    ...
