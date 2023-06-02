from etl.paths.components import Bucket, Dimension, Environment, Load, Tier
from etl.paths.create import create_path
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from holidays import Australia
from etl.jobs.optimised.attributes import with_slowly_changing_dimensions
import pandas as pd
import datetime

# date_dim | Creating date_dim table 

output_path:str = create_path(
    environment=Environment.PROD,
    bucket=Bucket.PROJECT,
    tier=Tier.OPTIMISED,
    dimension=Dimension.DATE,
    load=Load.FULL,
    time_requested='now'
)

def create_date_dimension() -> pd.DataFrame:

    start_date = datetime.date(2000, 1, 1)
    end_date = datetime.date(2030, 12, 31)

    # Set start and end year for the holiday generator
    start_year = start_date.year
    end_year = end_date.year

    # Generate the list of holidays that fall within the date range
    australian_holidays = []
    for year in range(start_year, end_year + 1):
        australian_holidays += Australia(years=year).items()

    # Convert the list of holidays into a set for faster lookup
    australia_holiday_set = set([holiday[0] for holiday in australian_holidays])

    # Generate a sequence of dates
    date_range = pd.date_range(start_date, end_date)

    # Create a DataFrame
    date_dim_df = pd.DataFrame({"date": date_range})

    # Add atomic date information.
    date_dim_df["date_key"] = date_dim_df["date"].apply(lambda x: int(x.strftime("%Y%m%d")))
    date_dim_df["day_of_week"] = date_dim_df["date"].apply(lambda x: x.weekday() + 1)
    date_dim_df["day_name"] = date_dim_df["date"].apply(lambda x: x.strftime("%A"))
    date_dim_df["day_of_month"] = date_dim_df["date"].apply(lambda x: x.day)
    date_dim_df["day_of_year"] = date_dim_df["date"].apply(lambda x: x.timetuple().tm_yday)
    date_dim_df["week_of_year"] = date_dim_df["date"].apply(lambda x: x.isocalendar()[1])
    date_dim_df["month"] = date_dim_df["date"].apply(lambda x: x.month)
    date_dim_df["month_name"] = date_dim_df["date"].apply(lambda x: x.strftime("%B"))
    date_dim_df["quarter"] = date_dim_df["date"].apply(lambda x: (x.month - 1) // 3 + 1)
    date_dim_df["year"] = date_dim_df["date"].apply(lambda x: x.year)
    date_dim_df["is_weekend"] = date_dim_df["day_of_week"].apply(lambda x: x in (6, 7))
    date_dim_df["is_weekday"] = date_dim_df["day_of_week"].apply(lambda x: x not in (6, 7))
    date_dim_df['is_holiday'] = date_dim_df['date'].isin(australia_holiday_set)

    # Format column order so that date_key is the first column
    date_dim_df = date_dim_df[
        [
            "date_key",
            "date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "day_of_year",
            "week_of_year",
            "month",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
            "is_weekday",
            "is_holiday"
        ]
    ]

    return date_dim_df


def transform_from_pandas_to_spark_dataframe(
            engine:SparkSession,
            panda_df:pd.DataFrame) -> DataFrame:

    # Create a spark dataframe from the pandas dataframe.
    spark_df = (engine
        .createDataFrame(panda_df)
        # Ensure `date` column is a yyyy-MM-dd natural key.
        .withColumn("date", F.date_format("date", "yyyy-MM-dd"))
    )
    transformed_df =  spark_df.withColumn("policyholder_dim_id", F.lit(None).cast("bigint"))
    
    return with_slowly_changing_dimensions(transformed_df)

def write_data(df:DataFrame, path:str, **kwargs):
    df.write.parquet(path, **kwargs)
