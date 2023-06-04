from etl.paths.components import Bucket, Source, Table, Dimension, Environment, Load, Tier
from etl.paths.create import create_path
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions
from pyspark.sql.functions import monotonically_increasing_id

# policyholder_dim | Access -> Optimised

def get_policyholder_input_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.ACCESS,
                       source=Source.CLAIM_DB,
                       table=Table.POLICYHOLDER,
                       load=Load.FULL,
                       time_requested='recent')

def get_location_input_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.OPTIMISED,
                       dimension=Dimension.LOCATION,
                       load=Load.FULL,
                       time_requested='recent')

def get_policyholder_output_path(env):
    return create_path(
    environment=Environment.AWS,
    bucket=env,
    tier=Tier.OPTIMISED,
    dimension=Dimension.POLICYHOLDER,
    load=Load.FULL,
    time_requested='now'
)

def read_parquet_data(engine:SparkSession,
            path:str,
            **kwargs):
    """
    Read data from a parquet file, returning a DataFrame.
    """
    df = engine.read.parquet(path, **kwargs)
    return df

def transform_data(policyholder_input:DataFrame,location_input:DataFrame) -> DataFrame:
    """
    Return a DataFrame with the distinct rows from `df1` and `df2`, with a monotonically increasing surrogate `surrogate_key`.
    """

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
    df.write.parquet(path, **kwargs)