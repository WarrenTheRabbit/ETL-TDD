import sys
import logging
from etl.schemas import landing as df_schema
from etl.jobs.landing import claim

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import SparkSession, DataFrame

def run(spark:SparkSession):
    # READ IN
    
    input_path = claim.get_input_path()
    output_path = claim.get_output_path()
    
    read_data = claim.read_data
    transform_data = claim.transform_data
    write_data = claim.write_data
    schema = df_schema.CLAIM
    
    read_df:DataFrame = read_data(
            engine=spark, 
            path=input_path, 
            schema=schema,
            header=True
    )
    
    # Visually validate the read dataframe.
    read_df.show(3, truncate=True)
        
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
    written_df.show(3, truncate=True)
            
    # JOB COMPLETED MESSAGE
    print(f"Finished running.")
    
    return written_df
    
if __name__ == '__main__':
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    run(spark)
    job.commit()