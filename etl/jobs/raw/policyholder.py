# claim_db.policyholder | Raw -> Access
# NOT DONE

import pyspark.sql.functions as F
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from etl.paths.components import Bucket, Source, Table, Environment, Load, Tier
from etl.paths.create import create_path

# A path to claim_db.policyholder table data that has been staged in Raw.
def get_input_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.RAW,
                       source=Source.CLAIM_DB,
                       table=Table.POLICYHOLDER,
                       load=Load.FULL,
                       time_requested='recent')

# A path for claim_db.policyholder table data to be staged to Access.
def get_output_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.ACCESS,
                       source=Source.CLAIM_DB,
                       table=Table.POLICYHOLDER,
                       load=Load.FULL,
                       time_requested='now')

def read_parquet_data(engine:SparkSession,
            path:str,
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
    Transform a DataFrame by splitting the 'address' column into multiple 
    columns.

    This function takes a DataFrame 'df' as input, and returns a DataFrame with 
    the same data, except for:
        - it has four additional columns created by extracting 'street', 
          'suburb', 'state', and 'postcode' columns from 'address'. The 
          'address' column is retained for use as a natural key.
        - mapping the values of `gender` to descriptive words instead of coded
          data (F -> Female, M -> Male, O -> Other)

    Parameters
    ----------
    df : DataFrame
        The input DataFrame.

    Returns
    -------
    transformed_df : DataFrame
        A DataFrame with the same data as 'df', but with additional 'street', 
        'suburb', 'state', and 'postcode' columns.
    """
    split_address = F.split(df['address'], ',')
    transformed_df = (df
        # Atomise address data but retain full entry as a natural key.
        .withColumn('street', split_address.getItem(0))
        .withColumn('suburb', split_address.getItem(1))
        .withColumn('state', split_address.getItem(2))
        .withColumn('postcode', split_address.getItem(3))
        # Conform gender values to descriptive words.
        .withColumn("gender", F.
                when(F.col("gender") == "F", "Female")
                .when(F.col("gender") == "M", "Male")
                .when(F.col("gender") == "O", "Other")
                .otherwise(F.col("gender"))
        )
    )
    return transformed_df, write_path


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
