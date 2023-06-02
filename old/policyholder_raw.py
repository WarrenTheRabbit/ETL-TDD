from pyspark.sql import SparkSession, DataFrame
from etl.jobs.raw.policyholder import input_path, output_path
from etl.jobs.raw.policyholder import read_parquet_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
        app_name = "claim_db.policyholder | Raw -> Access"
        print(f"{'':*^80}\nStarting application `{app_name}`...")

        # READ IN
        read_df:DataFrame = read_parquet_data(
                engine=spark, 
                path=input_path, 
                header=True
        )

        # Visually validate the read dataframe.
        print(f"Providing a visual check for the Raw data read from {input_path}\n")
        read_df.show(10, truncate=True)

        # TRANSFORM
        transformed_df:DataFrame = transform_data(read_df)

        # WRITE TO FILE
        write_data(
        df=transformed_df, 
        path=output_path, 
        mode='overwrite'
        )

        # Visually validate the written dataframe.
        written_df = spark.read.parquet(output_path)
        print(f"Checking Raw data written to {output_path}\n")
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