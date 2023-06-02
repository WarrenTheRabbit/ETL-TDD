import etl.jobs.landing.provider
import etl.jobs.raw.provider 
import etl.jobs.access.provider

from enum import Enum
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

import etl.jobs.landing.provider
import etl.jobs.raw.provider 
import etl.jobs.access.provider

expectations = {
    'Landing': {
        'Path': etl.jobs.landing.provider.output_path,
        'Count': 20,
        'Schema': "schema"
    },
    'Raw': {
        'Path': etl.jobs.raw.provider.get_input_path(),
        'Count': 5,
        'Schema': StructType(
                    [StructField('provider_id', IntegerType(), True), 
                    StructField('provider_name', StringType(), True), 
                    StructField('provider_address', StringType(), True), 
                    StructField('provider_phone_number', StringType(), True), 
                    StructField('provider_email_address', StringType(), True), 
                    StructField('provider_type', StringType(), True), 
                    StructField('provider_license_number', StringType(), True)]),
        'First Row': Row(
                    provider_id=558433, 
                    provider_name='Cox and Sons', 
                    provider_address='695 Ryan Nook, St. Patrick, VIC, 2697', 
                    provider_phone_number='+61 2 4485 8099', 
                    provider_email_address='rmcdowell@example.org', 
                    provider_type='Pharmacist', 
                    provider_license_number='82002543'),
        'Tail Row': Row(
                    provider_id=945989, 
                    provider_name='Morrison, Palmer and Adams', 
                    provider_address='Flat 00 575 Erica Key, Elizabethbury, NT, 9818', 
                    provider_phone_number='02-2869-3764', 
                    provider_email_address='matthewssamantha@example.org', 
                    provider_type='Specialist', 
                    provider_license_number='59185011')
    },
    'Access': {
        'Path': etl.jobs.access.provider.provider_dimension_output_path,
        'Count': 5,
        'Schema': StructType(
                    [StructField('provider_id', IntegerType(), True), 
                    StructField('provider_name', StringType(), True), 
                    StructField('provider_address', StringType(), True), 
                    StructField('provider_phone_number', StringType(), True), 
                    StructField('provider_email_address', StringType(), True), 
                    StructField('provider_type', StringType(), True), 
                    StructField('provider_license_number', StringType(), True)]),
                'First Row': Row(
                    provider_id=558433, 
                    provider_name='Cox and Sons', 
                    provider_address='695 Ryan Nook, St. Patrick, VIC, 2697', 
                    provider_phone_number='+61 2 4485 8099', 
                    provider_email_address='rmcdowell@example.org', 
                    provider_type='Pharmacist', 
                    provider_license_number='82002543'),
        'Tail Row': Row(
                    provider_id=945989, 
                    provider_name='Morrison, Palmer and Adams', 
                    provider_address='Flat 00 575 Erica Key, Elizabethbury, NT, 9818', 
                    provider_phone_number='02-2869-3764', 
                    provider_email_address='matthewssamantha@example.org', 
                    provider_type='Specialist', 
                    provider_license_number='59185011')
    },
        'Optimised': {
        'Path': etl.jobs.access.provider.provider_dimension_output_path,
        'Count': 5,
        'Schema': StructType(
                    [StructField('provider_id', IntegerType(), True), 
                    StructField('provider_name', StringType(), True), 
                    StructField('provider_address', StringType(), True), 
                    StructField('provider_phone_number', StringType(), True), 
                    StructField('provider_email_address', StringType(), True), 
                    StructField('provider_type', StringType(), True), 
                    StructField('provider_license_number', StringType(), True)]),
                'First Row': Row(
                    provider_id=558433, 
                    provider_name='Cox and Sons', 
                    provider_address='695 Ryan Nook, St. Patrick, VIC, 2697', 
                    provider_phone_number='+61 2 4485 8099', 
                    provider_email_address='rmcdowell@example.org', 
                    provider_type='Pharmacist', 
                    provider_license_number='82002543'),
        'Tail Row': Row(
                    provider_id=945989, 
                    provider_name='Morrison, Palmer and Adams', 
                    provider_address='Flat 00 575 Erica Key, Elizabethbury, NT, 9818', 
                    provider_phone_number='02-2869-3764', 
                    provider_email_address='matthewssamantha@example.org', 
                    provider_type='Specialist', 
                    provider_license_number='59185011')
    }
}


