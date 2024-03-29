from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.policyholder import get_input_path, get_output_path
from etl.jobs.raw.policyholder import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from etl.paths.components import Bucket

def run(spark: SparkSession, env:Bucket):
    
    # Read in data needed for transformations.
    read_df: DataFrame = read_parquet_data(engine=spark, 
                                           path=get_input_path(env), 
                                           header=True)
    
    # Apply transformations.  
    transformed_df: DataFrame = transform_data(read_df)

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


