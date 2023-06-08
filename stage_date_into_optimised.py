from etl.jobs.optimised.date_dim import get_output_path, create_date_dimension, \
    transform_from_pandas_to_spark_dataframe, write_data 
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from paths.components import Bucket
import validation.schemas.optimised as schemas

def run(spark:SparkSession, env):
   
    # Read in data needed for transformations.
    read_df:pd.DataFrame = create_date_dimension()

    # Apply transformations.  
    spark_df = transform_from_pandas_to_spark_dataframe(spark, 
                                                        read_df)

    # Write transformed data to path.
    write_path = get_output_path(env)
    write_data(df=spark_df, 
               path=write_path, 
               mode='overwrite') 
    
    return spark_df, write_path

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark,env=Bucket.PROD)
    job.commit()