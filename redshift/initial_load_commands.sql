COPY claim_fact
FROM 's3://project-dev-wm/optimised/claim_fact/full/202305231407'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;


COPY date_dim
FROM 's3://project-dev-wm/optimised/date_dim/full/202305231149'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;


COPY location_dim
FROM 's3://project-dev-wm/optimised/location_dim/full/202305231607'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;


COPY policyholder_dim
FROM 's3://project-dev-wm/optimised/policyholder_dim/full/202305231610/'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;


COPY procedure_dim
FROM 's3://project-dev-wm/optimised/procedure_dim/full/202305231614'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;


COPY provider_dim
FROM 's3://project-dev-wm/optimised/provider_dim/full/202305231613'
IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
FORMAT AS PARQUET;