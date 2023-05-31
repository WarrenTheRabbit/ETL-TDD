"""
This function runs the read, transform and write functions that are defined in 
the etl.jobs.landing.provider module. It reads data from the landing tier,
transforms it and writes it to the raw tier.
"""

from pyspark.sql import SparkSession, DataFrame
from etl.jobs.landing.provider import input_path, output_path
from etl.schemas import landing as schema
from etl.jobs.landing.provider import read_data, transform_data, write_data

app_name = "claim_db.provider | Landing -> Raw"
print(f"{'':*^80}\nStarting application `{app_name}`...")

# CONFIGURE
spark = (SparkSession
                .builder
                .master("local[*]")
                .appName(app_name)
                .getOrCreate())

# READ IN
read_df:DataFrame = read_data(engine=spark, path=input_path, 
                              schema=schema.PROVIDER, header=True)

# Visually validate the read dataframe.
print(f"Providing a visual check for the Landing data read from {input_path}\n")
read_df.show(10, truncate=True)

# Programatically validate the read dataframe.
expected_schema = schema.PROVIDER
assert read_df.schema == expected_schema
assert read_df.columns == expected_schema.fieldNames()
for field in expected_schema.fields:
    assert read_df.schema[field.name].dataType == field.dataType

# TRANSFORM
transformed_df:DataFrame = transform_data(read_df)

# WRITE TO FILE
write_data(df=transformed_df, path=output_path, mode='overwrite')

# Visually validate the written dataframe.
written_df = spark.read.parquet(output_path)
print(f"Checking Raw data written to {output_path}\n")
written_df.show(10, truncate=True)

# Programatically validate the written dataframe.
expected_schema = schema.PROVIDER
assert written_df.schema == expected_schema
assert written_df.columns == expected_schema.fieldNames()
for field in expected_schema.fields:
        assert written_df.schema[field.name].dataType == field.dataType
        
# JOB COMPLETED MESSAGE
print(f"Finished running `{app_name}`.")