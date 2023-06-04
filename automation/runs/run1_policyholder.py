
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext



def run(spark: SparkSession):
    import stage_source_into_landing
    stage_source_into_landing.run(spark,env)

    import stage_policyholder_into_raw
    stage_policyholder_into_raw.run(spark,env)

    import stage_policyholder_into_access
    stage_policyholder_into_access.run(spark,env)
    
if __name__ == '__main__':
    
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    run(spark)
    spark.stop()
