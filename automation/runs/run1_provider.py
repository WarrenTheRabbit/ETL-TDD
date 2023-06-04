
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import etl.mock.infrastructure.buckets as project
import etl.validation.output as validate
import etl.jobs as jobs
# import etl.jobs.validate.expectations.provider as provider


def run(spark: SparkSession):
    import stage_source_into_landing
    stage_source_into_landing.run(spark,env)

    import stage_provider_into_raw
    stage_provider_into_raw.run(spark,env)

    import stage_provider_into_access
    stage_provider_into_access.run(spark,env)
    
if __name__ == '__main__':
    
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    run(spark)
    spark.stop()
