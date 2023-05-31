from etl.paths.components import Bucket, Source, Dimension, Fact, Table, Environment, Load, Tier, create_path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from typing import List

# claim_dim | Access -> Optimised

claim_access_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.ACCESS,
    source=Source.CLAIM_DB,
    table=Table.CLAIM,
    load=Load.FULL,
    time_required='recent'
)
 
date_dim_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.DATE,
    load=Load.FULL,
    time_required='recent'
)

policyholder_dim_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.POLICYHOLDER,
    load=Load.FULL,
    time_required='recent'
)

provider_dim_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.PROVIDER,
    load=Load.FULL,
    time_required='recent'
) 

procedure_dim_input_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.PROCEDURE,
    load=Load.FULL,
    time_required='recent'
)

claim_fact_output_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    fact=Fact.CLAIM,
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

def transform_data(claim_access_df: DataFrame,
                   date_dim_df: DataFrame,
                   policyholder_dim_df: DataFrame,
                   provider_dim_df: DataFrame,
                   procedure_dim_df: DataFrame) -> DataFrame:
    """
    """
    return (
        claim_access_df
        # Join the claim acces table to the required dimension tables on their
        # natural keys. 
         .join(date_dim_df,
            on=claim_access_df.date_of_service == date_dim_df.date,
            how='left')
        .join(policyholder_dim_df,
            on= claim_access_df.policy_holder_id 
             == policyholder_dim_df.policy_holder_id,
            how='left')
        .join(provider_dim_df, 
            on=claim_access_df.provider_id == provider_dim_df.provider_id, 
            how='left')
        .join(procedure_dim_df,
            on=claim_access_df.procedure == procedure_dim_df.procedure,
            how='left')
        # Select the required facts from the claim access table and the 
        # surrogate keys from the dimension tables.
        .select(
                'claim_id',
                'provider_key',
                'policyholder_key',
                'procedure_key',
                'date_key',
                'total_procedure_cost',
                'medibank_pays',
                'medicare_pays',
                'excess',
                'out_of_pocket')
    )
