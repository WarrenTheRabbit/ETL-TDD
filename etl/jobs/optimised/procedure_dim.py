from etl.paths.components import Bucket, Source, Dimension, Table, Environment, Load, Tier, create_path
from pyspark.sql import SparkSession, DataFrame
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions 
from pyspark.sql.functions import monotonically_increasing_id

# procedure_dim | Access -> Optimised

claim_input_path = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.ACCESS,
    source=Source.CLAIM_DB,
    table=Table.CLAIM,
    load=Load.FULL,
    time_required='recent'
)

procedure_dim_output_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.OPTIMISED,
    tier=Tier.OPTIMISED,
    dimension=Dimension.PROCEDURE,
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


def transform_data(df:DataFrame):
    """
    """
    transformed_df = (df
            .select('procedure')
            .distinct()
            .withColumn('procedure_key', monotonically_increasing_id())
    )  
    return with_slowly_changing_dimensions(transformed_df)             