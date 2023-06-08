# claim_db.provider | Raw -> Access
# NOT DONE

import pyspark.sql.functions as F
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from paths.components import Bucket, Source, Table, Environment, Load, Tier
from paths.create import create_path

# A path to claim_db.provider table data that has been staged in Raw.
def get_input_path(env): 
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.RAW,
                       source=Source.CLAIM_DB,
                       table=Table.PROVIDER,
                       load=Load.FULL,
                       time_requested='recent')

# A path for claim_db.provider table data to be staged to Access.
def get_output_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.ACCESS,
                       source=Source.CLAIM_DB,
                       table=Table.PROVIDER,
                       load=Load.FULL,
                       time_requested='now')

def read_parquet_data(engine:SparkSession,
            path:str,
            schema:Optional[StructType]=None,
            **kwargs):
    """
    Read data from a parquet file, returning a DataFrame.
    
    Parameters
    ----------
    engine: SparkSession
        A SparkSession object.
    path: str
        The path to the CSV file.
    **kwargs
        Any other keyword arguments to pass to the engine's parquet reader.
        
    Returns
    -------
    df: DataFrame
        A DataFrame with the loaded data.
    """
    df = engine.read.parquet(path, **kwargs)
    return df

def transform_data(df:DataFrame) -> DataFrame:
    """
    Transform a DataFrame by splitting the 'provider_address' column into 
    multiple columns.

    This function takes a DataFrame `df` as input, and returns a DataFrame with 
    the same data, but with the 'provider_address' column split into four 
    separate columns: *street*, 'suburb', 'state', and 'postcode'. The 
    'provider_address' column is retained as a natural key.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame.

    Returns
    -------
    transformed_df : DataFrame
        A DataFrame with the same data as 'df', but with 'street', 'suburb', 
        'state', and 'postcode' columns added from information contained within
        the 'provider address' column.
    """
    split_address = F.split(df['provider_address'], ',')
    transformed_df = (df
        .withColumn('street', split_address.getItem(0))
        .withColumn('suburb', split_address.getItem(1))
        .withColumn('state', split_address.getItem(2))
        .withColumn('postcode', split_address.getItem(3))
        # rename the 'provider_address' column to 'address'
        .withColumnRenamed('provider_address', 'address')
    )
    return transformed_df


def write_data(df:DataFrame, path:str, **kwargs):
    """
    Write a DataFrame to a parquet file.

    This function takes a DataFrame 'df' and a path 'path' as input, and writes 
    the DataFrame to a parquet file at the specified path.

    Parameters
    ----------
    df : DataFrame
        The DataFrame to be written to a parquet file.

    path : str
        The path where the parquet file will be written.

    **kwargs : 
        Any other keyword arguments to pass to the DataFrame's parquet writer.
    """
    df.write.parquet(path, **kwargs)
