
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import etl.mock.infrastructure.buckets as project
import etl.validation.output as validate
import etl.jobs as jobs
import sys
from etl.mock.infrastructure.s3_bucket import get_mock_s3_server_and_its_local_process


def run(spark: SparkSession):
    import stage_source_into_landing
    stage_source_into_landing.run()

    import stage_date_into_optimised
    stage_date_into_optimised.run(spark)
    
if __name__ == '__main__':
    
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    run(spark)
    spark.stop()
