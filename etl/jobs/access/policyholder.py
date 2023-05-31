from etl.paths.components import Bucket, Source, Table, Dimension, Environment, Load, Tier, create_path
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions
from pyspark.sql.functions import current_timestamp, from_unixtime, lit, monotonically_increasing_id, unix_timestamp

# policyholder_dim | Access -> Optimised

policyholder_table_input_path = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.ACCESS,
    source=Source.CLAIM_DB,
    table=Table.POLICYHOLDER,
    load=Load.FULL,
    time_required='recent'
)

location_dim_input_path = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.LOCATION,
    load=Load.FULL,
    time_required='recent'
)

policyholder_dimension_output_path = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.POLICYHOLDER,
    load=Load.FULL,
    time_required='now'
)

def read_parquet_data(engine:SparkSession,
            path:str,
            **kwargs):
    """
    Read data from a parquet file, returning a DataFrame.
    """
    print(f"Reading data from the {path} parquet file...")
    print(f"\tdf = engine.read.parquet({path}, {kwargs})")
    df = engine.read.parquet(path, **kwargs)
    return df

def transform_data(policyholder_input:DataFrame,location_input:DataFrame) -> DataFrame:
    """
    Return a DataFrame with the distinct rows from `df1` and `df2`, with a monotonically increasing surrogate `surrogate_key`.
    """
    print(f"Transforming data...")
        
    transformed_df = (
        policyholder_input
        .withColumn('policyholder_key', monotonically_increasing_id())
        .join(location_input, on=['address'], how='inner')
        .select(
            "policyholder_key",
            "location_key",
            "policy_holder_id",
            "first_name",
            "last_name",
            "gender",
            "phone_number",
            "email_address",
            "date_of_birth",
            "insurance_plan_details",
            "policy_standing",
            "coverage_start_date",
            "coverage_end_date")        
    )    
    return with_slowly_changing_dimensions(transformed_df)

def write_data(df:DataFrame, path:str, **kwargs):
    print(f"Writing data to the {path} parquet file...")
    df.write.parquet(path, **kwargs)