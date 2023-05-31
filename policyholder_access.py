from pyspark.sql import SparkSession, DataFrame
from etl.jobs.access.policyholder import policyholder_table_input_path, policyholder_dimension_output_path, location_dim_input_path
from etl.jobs.access.policyholder import read_parquet_data, transform_data, write_data

app_name = "claim_db.policyholder | Access -> Optimised"
print(f"{'':*^80}\nStarting application `{app_name}`...")

# CONFIGURE
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName(app_name)
        .getOrCreate()
)

# READ IN
policyholder_df:DataFrame = read_parquet_data(
        engine=spark, 
        path=policyholder_table_input_path
)

location_dim_df:DataFrame = read_parquet_data(
        engine=spark,
        path=location_dim_input_path
)

# Visually validate the read dataframe.
print(f"Providing a visual check for the Access data read from {policyholder_table_input_path}\n")
policyholder_df.show(10, truncate=True)
location_dim_df.show(10, truncate=True)

# TRANSFORM
transformed_df:DataFrame = transform_data(policyholder_df, location_dim_df)

# WRITE TO FILE
write_data(
    df=transformed_df, 
    path=policyholder_dimension_output_path, 
    mode='overwrite'
)

# Visually validate the written dataframe.
written_df = spark.read.parquet(policyholder_dimension_output_path)
print(f"Checking Optimised data written to {policyholder_dimension_output_path}\n")
written_df.show(10, truncate=True)
        
# JOB COMPLETED MESSAGE
print(f"Finished running `{app_name}`.")