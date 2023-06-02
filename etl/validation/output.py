from pyspark.sql import SparkSession

def output(*,engine:SparkSession, 
                    path_to_dataframe:str,
                    expected_count,
                    expected_schema,
                    expected_top,
                    expected_bottom):
    df = engine.read.parquet(path_to_dataframe)
    print(f"{df.count()} == {expected_count}")
    assert df.count() == expected_count
    print(f"{df.schema} == {expected_schema}")
    assert df.schema == expected_schema
    print(f"{df.schema} == {expected_schema}")
    assert df.first() == expected_top
    print(f"{df.schema} == {expected_schema}")
    assert df.tail(1)[0] == expected_bottom
    
    return True