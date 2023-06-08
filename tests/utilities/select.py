from pyspark.sql import DataFrame

def select_non_timestamp_columns(df: DataFrame):
    non_ts_cols = [col.name for col in df.schema.fields if not col.name.endswith('_ts')]
    return df.select(non_ts_cols)
