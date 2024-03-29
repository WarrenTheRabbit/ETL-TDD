from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DecimalType

CLAIM:StructType = StructType(
        [
            StructField("claim_id", IntegerType(), True),
            StructField("policy_holder_id", IntegerType(), True),
            StructField("provider_id", IntegerType(), True),
            StructField("date_of_service", StringType(), True),
            StructField("procedure", StringType(), True),
            StructField("total_procedure_cost", FloatType(), True),
            StructField("medibank_pays", FloatType(), True),
            StructField("medicare_pays", FloatType(), True),
            StructField("excess", FloatType(), True),
            StructField("out_of_pocket", FloatType(), True)
        ])

POLICYHOLDER:StructType = StructType(
    [
        StructField("policy_holder_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("insurance_plan_details", StringType(), True),
        StructField("coverage_start_date", StringType(), True),
        StructField("coverage_end_date", StringType(), True),
        StructField("policy_standing", StringType(), True)
    ])

PROVIDER:StructType = StructType(
    [
        StructField("provider_id", IntegerType()),
        StructField("provider_name", StringType()),
        StructField("provider_address", StringType()),
        StructField("provider_phone_number", StringType()),
        StructField("provider_email_address", StringType()),
        StructField("provider_type", StringType()),
        StructField("provider_license_number", StringType())
    ])
