import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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

def run(spark, env):
    print(env)
    
 
if __name__ == "__main__":

    # Initialize environment based on command line arguments
    args = [arg.lower() for arg in sys.argv]

    if 'mock' in args:
        env = Bucket.MOCK
    elif 'prod' in args:
        env = Bucket.PROD
    elif 'test' in args:
        env = Bucket.TEST
    # Default value if no specific argument is provided
    else:
        env = Bucket.PROD 
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark, env)
    job.commit()
