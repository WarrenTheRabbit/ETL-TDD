import stage_provider_into_optimised
import stage_policyholder_into_optimised
from etl.paths.components import Bucket
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark,env)
    
    # Run 3 starts here.
    df = stage_policyholder_into_optimised.run(spark,env)
    assert True

    df = stage_provider_into_optimised.run(spark,env)
    assert True
    
if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark,env=Bucket.PROD)
    job.commit()