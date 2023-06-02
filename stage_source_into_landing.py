from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.claim import get_input_path, get_output_path
from etl.jobs.raw.claim import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run():
    pass

/home/glue_user/project_lf/ETL-TDD/stage_claim_into_access.py
/home/glue_user/project_lf/ETL-TDD/stage_claim_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_claim_into_raw.py
/home/glue_user/project_lf/ETL-TDD/stage_date_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_location_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_policyholder_into_access.py
/home/glue_user/project_lf/ETL-TDD/stage_policyholder_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_policyholder_into_raw.py
/home/glue_user/project_lf/ETL-TDD/stage_procedure_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_provider_into_access.py
/home/glue_user/project_lf/ETL-TDD/stage_provider_into_optimised.py
/home/glue_user/project_lf/ETL-TDD/stage_provider_into_raw.py
/home/glue_user/project_lf/ETL-TDD/stage_source_into_landing.py

if __name__ == 'main':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark)
    job.commit()