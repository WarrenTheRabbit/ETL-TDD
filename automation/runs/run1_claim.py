
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import etl.mock.infrastructure.buckets as project
import etl.validation.output as validate
import etl.jobs as jobs
# import etl.jobs.validate.expectations.provider as provider
import sys
from etl.mock.infrastructure.s3_bucket import get_mock_s3_server_and_its_local_process


def run(spark: SparkSession):
    import stage_source_into_landing
    stage_source_into_landing.run()

    import stage_claim_into_raw
    stage_claim_into_raw.run(spark)

    # if not validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.provider.get_input_path(),
    #             expected_count=provider.expectations['Raw']['Count'],
    #             expected_schema=provider.expectations['Raw']['Schema'],
    #             expected_top=provider.expectations['Raw']['First Row'],
    #             expected_bottom=provider.expectations['Raw']['Tail Row']
    #             ):
    #     raise Exception('Validation failed.')
    import stage_claim_into_access
    stage_claim_into_access.run(spark)
    # if not validate.output(engine=spark,
    #             path_to_dataframe=jobs.access.provider.get_input_path(),
    #             expected_count=provider.expectations['Access']['Count'],
    #             expected_schema=provider.expectations['Access']['Schema'],
    #             expected_top=provider.expectations['Access']['First Row'],
    #             expected_bottom=provider.expectations['Access']['Tail Row']
    #             ):
    #     raise Exception('Validation failed.')
    
if __name__ == '__main__':
    
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    run(spark)
    spark.stop()
