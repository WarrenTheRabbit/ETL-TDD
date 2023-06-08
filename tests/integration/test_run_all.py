import sys

import pytest
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import stage_source_into_landing
import stage_policyholder_into_raw
import stage_policyholder_into_access
import stage_claim_into_raw
import stage_claim_into_access
import stage_provider_into_raw
import stage_provider_into_access
import stage_date_into_optimised
import stage_location_into_optimised
import stage_procedure_into_optimised
import stage_policyholder_into_optimised
import stage_provider_into_optimised
import stage_claim_into_optimised

from paths.components import Bucket
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions
import validation.schemas as schemas
import validation.output as validate
from validation.expectations import date
from tests.utilities import select

test_actions = get_print_s3_actions()

@pytest.mark.integration
@infrastructure.with_test_server(test_actions)
def test_run(spark, env):
    # For real S3 buckets, get source files into landing.
    if env != Bucket.MOCK:
        stage_source_into_landing.run(spark, env)
    
    # Run 1 starts here. 
    df, _ = stage_policyholder_into_raw.run(spark, env)    
    df, _ = stage_policyholder_into_access.run(spark, env)
    df, _ = stage_claim_into_raw.run(spark, env)
    df, _ = stage_claim_into_access.run(spark, env)
    df, _ = stage_provider_into_raw.run(spark, env)
    df, _ = stage_provider_into_access.run(spark, env)

    # DATE DIMENSION
    df, _ = stage_date_into_optimised.run(spark, env)
    # is_valid = validate.output(df=df,
    #             expected_count=date.expectations['Optimised']['Count'],
    #             expected_schema=date.expectations['Optimised']['Schema'],
    #             expected_head=date.expectations['Optimised']['Head'],
    #             expected_tail=date.expectations['Optimised']['Tail'])
    # assert is_valid == True

    # Run 2 starts here.
    df, _ = stage_location_into_optimised.run(spark, env)
    df, _ = stage_procedure_into_optimised.run(spark, env)
    
    # Run 3 starts here.
    df, _ = stage_policyholder_into_optimised.run(spark, env)
    df, _ = stage_provider_into_optimised.run(spark, env)
    
    # Run 4 starts here.
    df, _ = stage_claim_into_optimised.run(spark, env)
    

