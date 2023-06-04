import stage_policyholder_into_raw
import stage_policyholder_into_access
import stage_claim_into_raw
import stage_claim_into_access
import stage_provider_into_raw
import stage_provider_into_access
import stage_date_into_optimised

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark,env)
    
    stage_policyholder_into_raw.run(spark,env)
    stage_policyholder_into_access.run(spark,env)
    stage_claim_into_raw.run(spark,env)
    stage_claim_into_access.run(spark,env)
    stage_provider_into_raw.run(spark,env)
    stage_provider_into_access.run(spark,env)
    stage_date_into_optimised.run(spark,env)
    
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark,env=Bucket.PROD)
    job.commit()