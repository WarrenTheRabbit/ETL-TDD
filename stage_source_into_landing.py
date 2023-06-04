from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.claim import get_input_path, get_output_path
from etl.jobs.raw.claim import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from etl.mock.infrastructure.mock_s3_server import S3ResourceSingleton
from etl.paths.components import Bucket

def run(spark, env):

    # Define source and target file paths
    files = {
        "/home/glue_user/project_lf/ETL-TDD/etl/mock/source_files/202305211851-claim-full.csv": f"s3://{env}/etl/landing/claim_db/claim/full/202305211851-claim-full.csv",
        "/home/glue_user/project_lf/ETL-TDD/etl/mock/source_files/202305221132-policyholder-full.csv": f"s3://{env}/etl/landing/claim_db/policyholder/full/202305221132-policyholder-full.csv",
        "/home/glue_user/project_lf/ETL-TDD/etl/mock/source_files/202305221136-provider-full.csv": f"s3://{env}/etl/landing/claim_db/provider/full/202305221136-provider-full.csv",
    }
    
    # Get correct s3_resource.
    s3_resource = S3ResourceSingleton.getInstance()

    # Copy each file to S3
    for src, dest in files.items():
        parts = dest.split(f'/')
        bucket_name = parts[2]
        key = '/'.join(parts[3:])
        s3_resource.Bucket(bucket_name).upload_file(src, key)


if __name__ == 'main':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark,env=Bucket.PROD)
    job.commit()