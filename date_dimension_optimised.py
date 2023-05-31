from etl.jobs.optimised.date_dim import output_path, create_date_dimension, transform_from_pandas_to_spark_dataframe, write_data 
from pyspark.sql import SparkSession, DataFrame
import pandas as pd

# -----------------------------date_dim--------------------------------------
# ------------------------------OPTIMISED---------------------------------
app_name = "creating date dimension table in Optimised tier"
print(f"{'':*^80}\nStarting application `{app_name}`...")

# CONFIGURE
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName(app_name)
        .getOrCreate()
)

# READ IN
read_df:pd.DataFrame = create_date_dimension()

# Visually validate the read dataframe.
print("Providing a visual check for the pandas Dataframe.\n")
print(read_df.head(10))

# CONVERT TO SPARK
spark_df = transform_from_pandas_to_spark_dataframe(spark, read_df)

# WRITE TO FILE
write_data(
    df=spark_df, 
    path=output_path, 
    mode='overwrite'
)

# Visually validate the written dataframe.
written_df = spark.read.parquet(output_path)
print(f"Checking date dimension table written to {output_path}\n")
written_df.show(10, truncate=True)
        
# JOB COMPLETED MESSAGE
print(f"Finished running `{app_name}`.")