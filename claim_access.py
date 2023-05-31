from pyspark.sql import SparkSession, DataFrame
from etl.jobs.access.claim import date_dim_input_path, claim_access_input_path, \
    policyholder_dim_input_path, procedure_dim_input_path, \
    provider_dim_input_path, claim_fact_output_path
from etl.jobs.access.claim import read_parquet_data, transform_data, write_data

app_name = "claim_db.claim | Access -> Optimised"
print(f"{'':*^80}\nStarting application `{app_name}`...")

# CONFIGURE
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName(app_name)
        .getOrCreate()
)

# READ IN
claim_access_df:DataFrame = read_parquet_data(
        engine=spark, 
        path=claim_access_input_path
)

date_dim_df:DataFrame = read_parquet_data(
        engine=spark,
        path=date_dim_input_path
)

policyholder_dim_df:DataFrame = read_parquet_data(
        engine=spark,
        path=policyholder_dim_input_path
)

procedure_dim_df:DataFrame = read_parquet_data(
        engine=spark,
        path=procedure_dim_input_path
)

provider_dim_df:DataFrame = read_parquet_data(
        engine=spark,
        path=provider_dim_input_path    
)


# Visually validate the read dataframe.
print(f"Providing a visual check for the dataframes read in.\n")
claim_access_df.show(5, truncate=False)
date_dim_df.show(5, truncate=False)
policyholder_dim_df.show(5, truncate=False)
procedure_dim_df.show(5, truncate=False)
provider_dim_df.show(5, truncate=False)


# TRANSFORM
transformed_df:DataFrame = transform_data(
                        claim_access_df=claim_access_df,
                        date_dim_df=date_dim_df,
                        policyholder_dim_df=policyholder_dim_df,
                        procedure_dim_df=procedure_dim_df,
                        provider_dim_df=provider_dim_df                
)

# WRITE TO FILE
write_data(
    df=transformed_df, 
    path=claim_fact_output_path, 
    mode='overwrite'
)

# Visually validate the written dataframe.
written_df = spark.read.parquet(claim_fact_output_path)
print(f"Checking Optimised data written to {claim_fact_output_path}\n")
written_df.show(10, truncate=True)
        
# JOB COMPLETED MESSAGE
print(f"Finished running `{app_name}`.")