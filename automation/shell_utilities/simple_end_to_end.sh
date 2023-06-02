#!/bin/bash

# Depends on claim source data
python3 claim_landing.py --JOB_NAME test && python3 claim_raw.py --JOB_NAME test

# Depends on policyholder source data
python3 policyholder_landing.py --JOB_NAME test && python3 policyholder_raw.py --JOB_NAME test

# Depends on provider source data
python3 provider_landing.py --JOB_NAME test && python3 provider_raw.py --JOB_NAME test

# No dependencies
python3 date_dimension_optimsied.py --JOB_NAME test

# Depends on policyholder_raw.py and provider_raw.py
python3 location_dimension_optimised.py --JOB_NAME test

# Depends on location_dimension_optimised.py
python3 provider_dim_optimised.py --JOB_NAME test

# Depends on location_dimension_optimised.py
python3 policyholder_dim_optimised.py --JOB_NAME test

# Depends on policyholder_dim_optimised.py and provider_dim_optimised.py
python3 claim_access.py --JOB_NAME test
