
# Sequential Dependencies
python3 claim_landing.py && python3 claim_raw.py 

# Sequential Dependencies
python3 policyholder_landing.py && python3 policyholder_raw.py 

# Sequential Dependencies
python3 provider_landing.py && python3 provider_raw.py

# No dependencies
python3 date_dimension_optimsied.py 

# Depends on policyholder_raw.py and provider_raw.py
python3 location_dimension_optimised.py

# Depends on
python3 provider_dim_optimised.py

# Depends on 
pythono3 policyholder_dim_optimised.py

# Depends on 
python3 claim_fact_optimised.py