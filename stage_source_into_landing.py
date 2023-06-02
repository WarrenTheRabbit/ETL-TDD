from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.claim import get_input_path, get_output_path
from etl.jobs.raw.claim import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark):
    pass

if __name__ == 'main':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark)
    job.commit()