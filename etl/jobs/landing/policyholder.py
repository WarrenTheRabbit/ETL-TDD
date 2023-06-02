from etl.paths.components import Bucket, Source, Table, Environment, Load, Tier
from etl.paths.create import create_path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# claim_db.policyholder | Landing -> Raw

def get_input_path(): 
    return create_path(environment=Environment.PROD,
                       bucket=Bucket.LANDING,
                       tier=Tier.LANDING,
                       source=Source.CLAIM_DB,
                       table=Table.POLICYHOLDER,
                       load=Load.FULL,
                       time_requested='recent',
                       file_extension='.csv')

def get_output_path(): 
    return create_path(environment=Environment.PROD,
                       bucket=Bucket.RAW,
                       tier=Tier.RAW,
                       source=Source.CLAIM_DB,
                       table=Table.POLICYHOLDER,
                       load=Load.FULL,
                       time_requested='now')                  

def read_data(engine:SparkSession,
            path:str,
            schema:StructType,
            **kwargs):
    
    if 'header' in kwargs:
        kwargs.pop('header')
    df = engine.read.csv(path, schema=schema, header=True, **kwargs)
    return df

def transform_data(df:DataFrame):
    return df

def write_data(df:DataFrame, path:str, **kwargs):
    df.write.parquet(path, **kwargs)


