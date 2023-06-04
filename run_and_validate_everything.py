
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import etl.mock.infrastructure.buckets as project
import etl.validation.output as validate
import etl.jobs as jobs
# import etl.jobs.validate.expectations.provider as provider
import sys

def run(spark: SparkSession, env):
    import stage_source_into_landing
    stage_source_into_landing.run(spark, env)

    # PROVIDER ------------------------------------------------------------
    import stage_provider_into_raw
    stage_provider_into_raw.run(spark,env)

    # if not validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.provider.get_input_path(env),
    #             expected_count=provider.expectations['Raw']['Count'],
    #             expected_schema=provider.expectations['Raw']['Schema'],
    #             expected_top=provider.expectations['Raw']['First Row'],
    #             expected_bottom=provider.expectations['Raw']['Tail Row']
    #             ):
    #     raise Exception('Validation failed.')
    import stage_provider_into_access
    stage_provider_into_access.run(spark,env)
    # if not validate.output(engine=spark,
    #             path_to_dataframe=jobs.access.provider.get_input_path(env),
    #             expected_count=provider.expectations['Access']['Count'],
    #             expected_schema=provider.expectations['Access']['Schema'],
    #             expected_top=provider.expectations['Access']['First Row'],
    #             expected_bottom=provider.expectations['Access']['Tail Row']
    #             ):
    #     raise Exception('Validation failed.')
    
    # POLICYHOLDER --------------------------------------------------------
    import stage_policyholder_into_access
    import stage_policyholder_into_raw
    import stage_claim_into_raw
    import stage_claim_into_access
    import stage_date_into_optimised
    import stage_procedure_into_optimised
    import stage_location_into_optimised
    import stage_policyholder_into_optimised
    import stage_provider_into_optimised
    import stage_claim_into_optimised
    stage_policyholder_into_raw.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.policyholder.get_input_path(env),
    #             expected_count=policyholder.expectations['Raw']['Count'],
    #             expected_schema=policyholder.expectations['Raw']['Schema'],
    #             expected_top=policyholder.expectations['Raw']['First Row'],
    #             expected_bottom=policyholder.expectations['Raw']['Tail Row']
    #             )
    print(".", end='')
    # RAW job
    stage_policyholder_into_access.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.policyholder.get_input_path(env),
    #             expected_count=policyholder.expectations['Access']['Count'],
    #             expected_schema=policyholder.expectations['Access']['Schema'],
    #             expected_top=policyholder.expectations['Access']['First Row'],
    #             expected_bottom=policyholder.expectations['Access']['Tail Row']
    #             )
    print(".", end='')
    # CLAIM ---------------------------------------------------------------
    stage_claim_into_raw.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #             expected_count=claim.expectations['Raw']['Count'],
    #             expected_schema=claim.expectations['Raw']['Schema'],
    #             expected_top=claim.expectations['Raw']['First Row'],
    #             expected_bottom=claim.expectations['Raw']['Tail Row']
    #             )
    print(".", end='')
    # RAW job
    stage_claim_into_access.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #             expected_count=claim.expectations['Access']['Count'],
    #             expected_schema=claim.expectations['Access']['Schema'],
    #             expected_top=claim.expectations['Access']['First Row'],
    #             expected_bottom=claim.expectations['Access']['Tail Row']
    #             )
    print(".", end='')
    stage_date_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #             expected_count=claim.expectations['Access']['Count'],
    #             expected_schema=claim.expectations['Access']['Schema'],
    #             expected_top=claim.expectations['Access']['First Row'],
    #             expected_bottom=claim.expectations['Access']['Tail Row']
    #             )
    print(".", end='')
    stage_procedure_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #             expected_count=claim.expectations['Access']['Count'],
    #             expected_schema=claim.expectations['Access']['Schema'],
    #             expected_top=claim.expectations['Access']['First Row'],
    #             expected_bottom=claim.expectations['Access']['Tail Row']
    #             )
    print(".", end='')
    stage_location_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #             path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #             expected_count=claim.expectations['Access']['Count'],
    #             expected_schema=claim.expectations['Access']['Schema'],
    #             expected_top=claim.expectations['Access']['First Row'],
    #             expected_bottom=claim.expectations['Access']['Tail Row']
    #             )
    print(".", end='')
    stage_policyholder_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #     path_to_dataframe=jobs.raw.policyholder.get_input_path(env),
    #     expected_count=policyholder.expectations['Optimised']['Count'],
    #     expected_schema=policyholder.expectations['Optimised']['Schema'],
    #     expected_top=policyholder.expectations['Optimised']['First Row'],
    #     expected_bottom=policyholder.expectations['Optimised']['Tail Row']
    #     )
    print(".", end='')
    stage_provider_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #     path_to_dataframe=jobs.raw.provider.get_input_path(env),
    #     expected_count=provider.expectations['Optimised']['Count'],
    #     expected_schema=provider.expectations['Optimised']['Schema'],
    #     expected_top=provider.expectations['Optimised']['First Row'],
    #     expected_bottom=provider.expectations['Optimised']['Tail Row']
    #     )
    print(".", end='')
    stage_claim_into_optimised.run(spark,env)
    # validate.output(engine=spark,
    #     path_to_dataframe=jobs.raw.claim.get_input_path(env),
    #     expected_count=claim.expectations['Optimised']['Count'],
    #     expected_schema=claim.expectations['Optimised']['Schema'],
    #     expected_top=claim.expectations['Optimised']['First Row'],
    #     expected_bottom=claim.expectations['Optimised']['Tail Row']
    #     )
    print(".", end='')
if __name__ == '__main__':
            
    glueContext = GlueContext(SparkContext.getOrCreate())
    spark = glueContext.spark_session
    run(spark)
    spark.stop()