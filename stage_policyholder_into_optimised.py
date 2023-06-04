from pyspark.sql import SparkSession
from etl.jobs.access.policyholder import get_policyholder_input_path, \
	get_policyholder_output_path, get_location_input_path
from etl.jobs.access.policyholder import read_parquet_data, transform_data, \
	write_data
    
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession, env):
        
	# Read in data needed for transformations.
	policyholder_df = read_parquet_data(engine=spark, 
										path=get_policyholder_input_path(env))
	
	location_dim_df = read_parquet_data(engine=spark,
										path=get_location_input_path(env))

	# Apply transformations.
	transformed_df = transform_data(policyholder_df, location_dim_df)

	# Write transformed data to path.
	write_path = get_policyholder_output_path(env)
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