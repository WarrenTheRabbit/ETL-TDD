from etl.jobs.optimised.location_dim import provider_input_path, \
    policyholder_input_path, read_parquet_data, transform_data, write_data, \
        location_dim_output_path
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

# -----------------------------date_dim--------------------------------------
# ------------------------------OPTIMISED---------------------------------
app_name = "creating location dimension table in Optimised tier"
print(f"{'':*^80}\nStarting application `{app_name}`...")

# CONFIGURE
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName(app_name)
        .getOrCreate()
)

# READ IN
policyholder_df:DataFrame = read_parquet_data(engine=spark, 
                                              path=policyholder_input_path)

provider_df:DataFrame = read_parquet_data(engine=spark, 
                                          path=provider_input_path)


# Visually validate the read dataframes.
print("Providing a visual check for the Dataframes.\n")
policyholder_df.show(5, truncate=True)
provider_df.show(5, truncate=True)

# TRANSFORM
transformed_df = transform_data(policyholder=policyholder_df,
                                provider=provider_df)

# Visually validate the transformed dataframe.
print("Providing a visual check for the transformed Dataframe.\n")
transformed_df.show(5, truncate=True)


# WRITE TO FILE
write_data(
    df=transformed_df, 
    path=location_dim_output_path, 
    mode='overwrite'
)

# Visually validate the written dataframe.
written_df = spark.read.parquet(location_dim_output_path)
print(f"Checking location dimension table written to {location_dim_output_path}\n")
written_df.show(10, truncate=True)
        
# JOB COMPLETED MESSAGE
print(f"Finished running `{app_name}`.")