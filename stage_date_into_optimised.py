from etl.jobs.optimised.date_dim import output_path, create_date_dimension, \
    transform_from_pandas_to_spark_dataframe, write_data 
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
   
    # Read in data needed for transformations.
    read_df:pd.DataFrame = create_date_dimension()

    # Apply transformations.  
    spark_df = transform_from_pandas_to_spark_dataframe(spark, read_df)

    # Write transformed data to path.
    write_data(df=spark_df, 
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