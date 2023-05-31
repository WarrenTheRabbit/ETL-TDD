import os
import boto3
import signal
import subprocess
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


SOURCE_NAME = "data.csv"
TABLE_NAME = "dummy"
S3_BUCKET_NAME = "data-s3"
ENDPOINT_URL = "http://127.0.0.1:5000/"


def initialize_test(spark: SparkSession):
    """
    Function to setup and initialize test case execution

    Args:
        spark (SparkSession): PySpark session object

    Returns:
        process: Process object for the moto server that was started
    """
    process = subprocess.Popen(
        "moto_server s3 -p5000",
        stdout=subprocess.PIPE,
        shell=True,
        preexec_fn=os.setsid,
    )

    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="FakeKey",
        aws_secret_access_key="FakeSecretKey",
        aws_session_token="FakeSessionToken",
        region_name="us-east-1",
    )
    s3.create_bucket(
        Bucket=S3_BUCKET_NAME,
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    values = [
        ("sam", "1962-05-25"),
        ("let", "1999-05-21"),
        ("nick", "1996-04-03"),
    ]
    columns = ["name", "dt"]
    df = spark.createDataFrame(values, columns)
    df.write.parquet(f"s3://{S3_BUCKET_NAME}/{SOURCE_NAME}")
    return process


spark = SparkSession.builder.appName("test").getOrCreate()
process = initialize_test(spark)
try: 
    df = spark.read.parquet(f"s3://{S3_BUCKET_NAME}/{SOURCE_NAME}")
    df.show()
    # Add new row
    new_row = spark.createDataFrame([("joe", "1999-05-21")], ["name", "dt"])
    new_row.show()
    # join dataframes
    df = df.union(new_row)
    df.show()
    spark.stop()
finally:
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)