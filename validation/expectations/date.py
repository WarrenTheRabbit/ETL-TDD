import etl.jobs.landing.provider
import etl.jobs.raw.provider 
import etl.jobs.access.provider

from enum import Enum
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from validation.schemas import optimised

expectations = {
    'Optimised': {
        'Count': 11323,
        'Schema': optimised.DATE,
        'Head': [Row(date_key=20000101,
                          date='2000-01-01',
                          day_of_week=6,
                          day_name='Saturday',
                          day_of_month=1,
                          day_of_year=1,
                          week_of_year=52,
                          month=1,
                          month_name='January',
                          quarter=1,
                          year=2000,
                          is_weekend=True,
                          is_weekday=False,
                          is_holiday=True,
                          track_hash=None,
                          record_active_flag=1)],
        'Tail': [Row(date_key=20301231,
                     date='2030-12-31',
                     day_of_week=2,
                     day_name='Tuesday',
                     day_of_month=31,
                     day_of_year=365,
                     week_of_year=1,
                     month=12,
                     month_name='December',
                     quarter=4,
                     year=2030,
                     is_weekend=False,
                     is_weekday=True,
                     is_holiday=False,
                     track_hash=None,
                     record_active_flag=1)]
    }
}


