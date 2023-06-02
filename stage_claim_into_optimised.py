import sys

from pyspark.sql import SparkSession, DataFrame
from etl.jobs.access.claim import get_date_dim_get_input_path, get_claim_access_get_input_path, \
    get_policyholder_dim_get_input_path, get_procedure_dim_get_input_path, \
    get_provider_dim_get_input_path, get_claim_fact_output_path
from etl.jobs.access.claim import read_parquet_data, transform_data, write_data

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
        
        # Read in data needed for transformations.
        claim_access_df = read_parquet_data(engine=spark, 
                                            path=get_claim_access_get_input_path())

        date_dim_df = read_parquet_data(engine=spark,
                                        path=get_date_dim_get_input_path())
        
        policyholder_dim_df = read_parquet_data(engine=spark,
                                                path=get_policyholder_dim_get_input_path())
        
        procedure_dim_df = read_parquet_data(engine=spark,
                                             path=get_procedure_dim_get_input_path())
        
        provider_dim_df = read_parquet_data(engine=spark,
                                            path=get_provider_dim_get_input_path())
       
        # Apply transformations.  
        transformed_df = transform_data(claim_access_df=claim_access_df,
                                        date_dim_df=date_dim_df,
                                        policyholder_dim_df=policyholder_dim_df,
                                        procedure_dim_df=procedure_dim_df,
                                        provider_dim_df=provider_dim_df)

        # Write transformed data to path.
        write_path = get_claim_fact_output_path()
        write_data(df=transformed_df, 
                   path=write_path, 
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