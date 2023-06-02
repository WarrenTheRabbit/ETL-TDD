"""
This function runs the read, transform and write functions that are defined in 
the etl.jobs.landing.provider module. It reads data from the landing tier,
transforms it and writes it to the raw tier.
"""
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.landing.provider import input_path, output_path
from etl.schemas import landing as schema
from etl.jobs.landing.provider import read_data, transform_data, write_data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


def run(spark:SparkSession):
    app_name = "claim_db.provider | Landing -> Raw"
    print(f"{'':*^80}\nStarting application `{app_name}`...")

    # READ IN
    read_df:DataFrame = read_data(engine=spark, path=input_path, 
                                schema=schema.PROVIDER, header=True)
    read_df.show(3, truncate=True)


    # TRANSFORM
    transformed_df:DataFrame = transform_data(read_df)

    # WRITE TO FILE
    write_data(df=transformed_df, path=output_path, mode='overwrite')
    written_df = spark.read.parquet(output_path)
    written_df.show(3, truncate=True)
            
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