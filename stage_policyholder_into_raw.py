from pyspark.sql import SparkSession, DataFrame
from etl.jobs.landing.policyholder import input_path, output_path
from etl.validation.schemas import landing as schema
from etl.jobs.landing.policyholder import read_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
    
    # Read in data needed for transformations.
    read_df:DataFrame = read_data(engine=spark, 
                                    path=input_path, 
                                    schema=schema.POLICYHOLDER,header=True)
    # Apply transformations.  
    transformed_df:DataFrame = transform_data(read_df)

    # Write transformed data to path.
    write_data(df=transformed_df, 
                path=output_path, 
                mode='overwrite')

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark)
    job.commit()
    
