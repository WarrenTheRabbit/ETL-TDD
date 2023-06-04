## Start Run 1

### Load data.
Inputs needed: 

Process: `stage_x_into_y`

Output: `df` and `full path`

### Create data catalog.
Inputs needed: name, role, database_name, prefix, s3_target_path

Process: glue.create_crawler()

Output: response = 

### Connect dataset to data catalog.
Inputs needed: name, table_name, database_name='test-release-3', CatalogId='618572314333'):

Process: create_dataset

Output: response

### Run profile job.
Inputs needed: 

Process: 

Output: `df` and `full path`

### Get profile job results.
Inputs needed: 

Process: 

Output: `df` and `full path`

### Create data quality job.
Inputs needed: 

Process: 

Output: `df` and `full path`

### Run data quality job.
Inputs needed: 

Process: 

Output: `df` and `full path`

### Get data quality job results.
Inputs needed: 

Process: 

Output: `df` and `full path`


## Start Run 2