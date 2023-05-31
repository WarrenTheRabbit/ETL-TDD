from etl.paths.components import Bucket, Source, Dimension, Table, Environment, Load, Tier, create_path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from typing import List
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions 


# location_dim | Access -> Optimised

provider_input_path = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.ACCESS,
    source=Source.CLAIM_DB,
    table=Table.PROVIDER,
    load=Load.FULL,
    time_required='recent'
)

policyholder_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.ACCESS,
    source=Source.CLAIM_DB,
    table=Table.POLICYHOLDER,
    load=Load.FULL,
    time_required='recent'
)

location_dim_output_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.OPTIMISED,
    tier=Tier.OPTIMISED,
    dimension=Dimension.LOCATION,
    load=Load.FULL,
    time_required='now')

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

def write_data(df:DataFrame, path:str, **kwargs):
    df.write.parquet(path, **kwargs)


def transform_data(provider:DataFrame, policyholder:DataFrame):
    """
    Return a DataFrame with the distinct rows from the union of `provider` and 
    `policyholder`, with a monotonically increasing surrogate surrogate key 
    and only address information selected.
    """
    provider = provider.select(
                    "address",
                    "street",
                    "postcode",
                    "suburb")
    
    policyholder = policyholder.select(
                    "address",
                    "street",
                    "postcode",
                     "suburb")
    
    transformed_df = (provider
               .union(policyholder)
               .distinct()
               .withColumn("location_key", monotonically_increasing_id())
               .select(
                    "location_key",
                    "address",
                    "street",
                    "postcode",
                    "suburb")
    )
    return with_slowly_changing_dimensions(transformed_df)