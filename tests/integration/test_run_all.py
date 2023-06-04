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

from etl.paths.components import Bucket
from tests.utilities import infrastructure
from tests.utilities.actions import get_print_s3_actions

test_actions = get_print_s3_actions()

@pytest.mark.integration
@infrastructure.with_test_server(test_actions)
def test_run(spark, env):
    # For real S3 buckets, get source files into landing.
    if env != Bucket.MOCK:
        stage_source_into_landing.run(spark, env)
    
    # Run 1 starts here. 
    stage_policyholder_into_raw.run(spark, env)
    stage_policyholder_into_access.run(spark, env)
    stage_claim_into_raw.run(spark, env)
    stage_claim_into_access.run(spark, env)
    stage_provider_into_raw.run(spark, env)
    stage_provider_into_access.run(spark, env)
    stage_date_into_optimised.run(spark, env)
   
    # Run 2 starts here.
    stage_location_into_optimised.run(spark, env)
    stage_procedure_into_optimised.run(spark, env)
    
    # Run 3 starts here.
    stage_policyholder_into_optimised.run(spark, env)
    stage_provider_into_optimised.run(spark, env)
    
    # Run 4 starts here.
    stage_claim_into_optimised.run(spark, env)
    

