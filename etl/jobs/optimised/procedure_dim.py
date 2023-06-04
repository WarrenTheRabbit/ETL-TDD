from etl.paths.components import Bucket, Source, Dimension, Table, Environment, Load, Tier
from etl.paths.create import create_path
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions 
from pyspark.sql.functions import monotonically_increasing_id

# procedure_dim | Access -> Optimised

def get_claim_input_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.ACCESS,
                       source=Source.CLAIM_DB,
                       table=Table.CLAIM,
                       load=Load.FULL,
                       time_requested='recent'
)

def get_procedure_dim_output_path(env):
    return create_path(environment=Environment.AWS,
                       bucket=env,
                       tier=Tier.OPTIMISED,
                       dimension=Dimension.PROCEDURE,
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

def write_data(df:DataFrame, path:str, **kwargs):
    df.write.parquet(path, **kwargs)


def transform_data(df:DataFrame):
    """
    """
    transformed_df = (df
            .select('procedure')
            .distinct()
            .withColumn('procedure_key', monotonically_increasing_id())
    )  
    return with_slowly_changing_dimensions(transformed_df)             