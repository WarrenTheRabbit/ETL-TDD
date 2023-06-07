from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DecimalType, BooleanType

DATE:StructType = StructType([
        StructField("date_key", IntegerType(), True),
        StructField("date", DateType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("day_of_month", IntegerType(), True),
        StructField("day_of_year", IntegerType(), True),
        StructField("week_of_year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("is_weekend", BooleanType(), True),
        StructField("is_weekday", BooleanType(), True),
        StructField("is_holiday", BooleanType(), True),
    ])