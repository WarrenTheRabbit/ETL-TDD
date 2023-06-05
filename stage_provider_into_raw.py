"""
This function runs the read, transform and write functions that are defined in 
the etl.jobs.landing.provider module. It reads data from the landing tier,
transforms it and writes it to the raw tier.
"""
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.landing.provider import get_input_path, get_output_path
from etl.paths.components import Bucket
from etl.validation.schemas import landing as schema
from etl.jobs.landing.provider import read_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def run(spark:SparkSession, env):

    # Read in data needed for transformations.
    read_path = get_input_path(env)
    read_df:DataFrame = read_data(engine=spark, 
                                  path=read_path, 
                                  schema=schema.PROVIDER, 
                                  header=True)
    # Apply transformations.  
    transformed_df:DataFrame = transform_data(read_df)

    # Write transformed data to path.
    write_path = get_output_path(env)
    write_data(df=transformed_df, 
               path=write_path, 
               mode='overwrite') 
    
    return transformed_df, write_path
    
if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)    
    run(spark,env=Bucket.PROD)
    job.commit()