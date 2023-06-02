# claim_db.claim | Raw -> Access
# NOT DONE

import pyspark.sql.functions as F
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from etl.paths.components import Bucket, Source, Table, Environment, Load, Tier, Fact
from etl.paths.create import create_path

# A path to the claim_db.claim table data that has been staged in Raw.
def get_input_path():
    return create_path(environment=Environment.PROD,
                    bucket=Bucket.RAW,
                    tier=Tier.RAW,
                    source=Source.CLAIM_DB,
                    table=Table.CLAIM,
                    load=Load.FULL,
                    time_requested='recent'
)

# A path for claim_db.claim table data to be staged to Access.
def get_output_path():
    return create_path(
        environment=Environment.PROD,
        bucket=Bucket.ACCESS,
        tier=Tier.ACCESS,
        source=Source.CLAIM_DB,
        table=Table.CLAIM,
        load=Load.FULL,
        time_requested='now'
    )

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
    No transformations are required for claim_db.claim table data to move 
    from Raw staging to Access staging.

    This function takes a DataFrame 'df' as input, and returns a DataFrame with 
    the same data.

    Parameters
    ----------
    df : DataFrame
        The input DataFrame.

    Returns
    -------
    transformed_df : DataFrame
        A DataFrame with the same data as the input 'df'.
    """
    return df


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
