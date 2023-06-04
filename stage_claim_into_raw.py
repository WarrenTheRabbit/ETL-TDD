import sys
from awsglue import DynamicFrame

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, DataFrame
from pyspark.context import SparkContext

from etl.validation.schemas import landing as claim_schema
from etl.jobs.landing import claim
from etl.jobs.landing.claim import read_data, transform_data, write_data, \
    get_input_path, get_output_path

def run(glueContext:GlueContext,env):
    
    # Read in data needed for transformations.    
    read_path = get_input_path(env)
    read_df:DataFrame = read_data(engine=glueContext.spark_session, 
                                  path=read_path, 
                                  schema=claim_schema.CLAIM,
                                  header=True)

    # Apply transformations.  
    transformed_df:DataFrame = transform_data(read_df)
    
    # Write transformed data to path.
    write_path = get_output_path(env)
    write_data(df=transformed_df, 
               path=write_path, 
               mode='overwrite') 
    
    # Return transformed data for optional testing.
    
    # return as a dynamic frame
    dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dyf")
    
    return dynamic_frame, write_path
    
if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(glueContext,env=Bucket.PROD)
    job.commit()