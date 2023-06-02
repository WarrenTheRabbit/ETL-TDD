from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.claim import get_input_path, get_output_path
from etl.jobs.raw.claim import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
        
        # Read in data needed for transformations.
        read_path = get_input_path()
        read_df:DataFrame = read_parquet_data(engine=spark, 
                                              path=read_path)

        # Apply transformations.        
        transformed_df:DataFrame = transform_data(read_df)

        # Write transformed data to path.
        write_path = get_output_path()
        write_data(df=transformed_df, 
                   path=write_path, 
                   mode='overwrite')

if __name__ == "__main__":
        args = getResolvedOptions(sys.argv, ['JOB_NAME'])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        job = Job(glueContext)
        job.init(args['JOB_NAME'], args)
        run(spark)
        job.commit()