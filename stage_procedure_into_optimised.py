from etl.jobs.optimised.procedure_dim import get_claim_input_path, \
    get_procedure_dim_output_path, read_parquet_data, transform_data, write_data
from pyspark.sql import SparkSession, DataFrame

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession, env):
    
    # Read in data needed for transformations.
    claim_df:DataFrame = read_parquet_data(engine=spark, 
                                           path=get_claim_input_path(env))
    
    # Apply transformations.  
    transformed_df = transform_data(df=claim_df)

    # Write transformed data to path.
    write_path = get_procedure_dim_output_path(env)
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