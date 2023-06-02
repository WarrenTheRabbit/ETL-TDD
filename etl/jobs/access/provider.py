from etl.paths.components import Bucket, Source, Table, Dimension, \
    Environment, Load, Tier
from etl.paths.create import create_path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit,monotonically_increasing_id
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions 
from typing import List

# provider_dim | Access -> Optimised

def get_input_path():
    return create_path(environment=Environment.PROD,
                       bucket=Bucket.PROJECT,
                       tier=Tier.ACCESS,
                       source=Source.CLAIM_DB,
                       table=Table.PROVIDER,
                       load=Load.FULL,
                       time_requested='recent')

def get_location_input_path():
    return create_path(environment=Environment.PROD,
                       bucket=Bucket.PROJECT,
                       tier=Tier.OPTIMISED,
                       dimension=Dimension.LOCATION,
                       load=Load.FULL,
                       time_requested='recent')

def get_provider_output_path():
    return create_path(environment=Environment.PROD,
                       bucket=Bucket.PROJECT,
                       tier=Tier.OPTIMISED,
                       dimension=Dimension.PROVIDER,
                       load=Load.FULL,
                       time_requested='now')

def read_parquet_data(engine:SparkSession,
            path:str,
            **kwargs):
    """
    Read data from a parquet file, returning a DataFrame.
    """
    df = engine.read.parquet(path, **kwargs)
    return df

def transform_data(provider_input:DataFrame,location_input:DataFrame) -> DataFrame:
    """
    Return a DataFrame with the distinct rows from `df1` and `df2`, with a monotonically increasing surrogate `surrogate_key`.
    """        
    transformed_df = (
        provider_input
        .withColumn('provider_key', monotonically_increasing_id())
        .join(location_input, on=['address'], how='inner')
        .select(
            'provider_key',
            'location_key',
            'provider_license_number',
            'provider_id',
            'provider_name',
            'provider_phone_number',
            'provider_email_address',
            'provider_type'
        )
    )    
    return with_slowly_changing_dimensions(transformed_df)

def write_data(df:DataFrame, path:str, **kwargs):
    df.write.parquet(path, **kwargs)