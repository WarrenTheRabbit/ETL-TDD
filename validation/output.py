from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from tests.utilities.select import select_non_timestamp_columns

def output(*,df,expected_count,
                expected_schema,
                expected_head,
                expected_tail):
    
    assert df.count() == expected_count
    assert df.schema == expected_schema
    
    df = select_non_timestamp_columns(df)
    assert df.head(1) == expected_head
    assert df.tail(1) == expected_tail
    
    return True