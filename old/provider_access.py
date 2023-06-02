from pyspark.sql import SparkSession, DataFrame
from etl.jobs.access.provider import provider_table_get_input_path, provider_dimension_output_path, get_location_input_path
from etl.jobs.access.provider import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
        app_name = "claim_db.provider | Access -> Optimised"
        print(f"{'':*^80}\nStarting application `{app_name}`...")

        # READ IN
        provider_df:DataFrame = read_parquet_data(
                engine=spark, 
                path=provider_table_get_input_path
        )

        location_dim_df:DataFrame = read_parquet_data(
                engine=spark,
                path=get_location_input_path
        )

        # Visually validate the read dataframe.
        print(f"Providing a visual check for the Access data read from {provider_table_get_input_path}\n")
        provider_df.show(10, truncate=True)
        location_dim_df.show(10, truncate=True)

        # TRANSFORM
        transformed_df:DataFrame = transform_data(provider_df, location_dim_df)

        # WRITE TO FILE
        write_data(
        df=transformed_df, 
        path=provider_dimension_output_path, 
        mode='overwrite'
        )

        # Visually validate the written dataframe.
        written_df = spark.read.parquet(provider_dimension_output_path)
        print(f"Checking Optimised data written to {provider_dimension_output_path}\n")
        written_df.show(10, truncate=True)
                
        # JOB COMPLETED MESSAGE
        print(f"Finished running `{app_name}`.")

if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark)
    job.commit()
    
    