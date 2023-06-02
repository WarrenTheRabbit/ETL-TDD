from pyspark.sql import SparkSession, DataFrame
from etl.jobs.access.policyholder import get_policyholder_input_path, get_policyholder_output_path, get_location_input_path
from etl.jobs.access.policyholder import read_parquet_data, transform_data, write_data
    
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
        
        app_name = "claim_db.policyholder | Access -> Optimised"
        print(f"{'':*^80}\nStarting application `{app_name}`...")

        # READ IN
        policyholder_df:DataFrame = read_parquet_data(
                engine=spark, 
                path=get_policyholder_input_path()
        )

        location_dim_df:DataFrame = read_parquet_data(
                engine=spark,
                path=get_location_input_path()
        )

        # Visually validate the read dataframe.
        policyholder_df.show(10, truncate=True)
        location_dim_df.show(10, truncate=True)

        # TRANSFORM
        transformed_df:DataFrame = transform_data(policyholder_df, location_dim_df)

        # WRITE TO FILE
        write_path = get_policyholder_output_path()
        write_data(
        df=transformed_df, 
        path=write_path, 
        mode='overwrite'
        )

        # Visually validate the written dataframe.
        written_df = spark.read.parquet(write_path)
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