from etl.jobs.optimised.procedure_dim import claim_get_input_path, \
    procedure_dim_output_path, read_parquet_data, transform_data, write_data
from pyspark.sql import SparkSession, DataFrame

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def run(spark:SparkSession):
    app_name = "creating procedure dimension table in Optimised tier"
    print(f"{'':*^80}\nStarting application `{app_name}`...")

    # READ IN
    claim_df:DataFrame = read_parquet_data(engine=spark, 
                                                path=claim_get_input_path)

    # Visually validate the read dataframes.
    print("Providing a visual check for the Dataframes.\n")
    claim_df.show(5, truncate=True)

    # TRANSFORM
    transformed_df = transform_data(df=claim_df)

    # Visually validate the transformed dataframe.
    print("Providing a visual check for the transformed Dataframe.\n")
    transformed_df.show(5, truncate=True)


    # WRITE TO FILE
    write_data(
        df=transformed_df, 
        path=procedure_dim_output_path, 
        mode='overwrite'
    )

    # Visually validate the written dataframe.
    written_df = spark.read.parquet(procedure_dim_output_path)
    print(f"Checking location dimension table written to {procedure_dim_output_path}\n")
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