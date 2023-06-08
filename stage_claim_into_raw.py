import sys
from awsglue import DynamicFrame

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext
from paths.components import Bucket

from validation.schemas import landing as claim_schema
from etl.jobs.landing.claim import read_data, transform_data, write_data, \
    get_input_path, get_output_path

def run(spark:SparkSession,env):
    
    # Read in data needed for transformations.    
    read_path = get_input_path(env)
    read_df:DataFrame = read_data(engine=spark, 
                                  path=read_path, 
                                  schema=claim_schema.CLAIM,
                                  header=True)

    # Apply transformations.  
    transformed_df:DataFrame = transform_data(read_df)
    
    # Write dynamic frame to path.
    write_path = get_output_path(env)
    write_data(df=transformed_df, 
               path=write_path, 
               mode='overwrite') 
    
    # Return transformed data for optional testing, and write_path for 
    # basing automation tasks on the s3 path.  
    return transformed_df, write_path
    
if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark = glueContext.spark_session
    run(spark, env=Bucket.PROD)
    job.commit()