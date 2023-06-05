
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, from_unixtime, lit, monotonically_increasing_id, unix_timestamp

def with_slowly_changing_dimensions(df:DataFrame) -> DataFrame:
    return (df
        .withColumn("track_hash", lit(None).cast("bigint"))
        .withColumn("record_start_ts", lit("1970-01-01 00:00:00").cast("string")) 
        .withColumn("record_end_ts", lit("2999-12-31 00:00:00").cast("string")) 
        .withColumn("record_active_flag", lit(1).cast("smallint"))
        .withColumn("record_upd_ts", current_timestamp().cast("string"))
        .withColumn("record_insert_ts", current_timestamp().cast("string"))
    )
