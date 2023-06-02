
GET ALL APPLICATIONS
aws emr-serverless list-appliations --region us-east-1

FROM ALL APPLICATIONS, IDENTITY `application-id`
aws emr-serverless get-application --region us-east-1 --application-id 00fahd28ijaete09

GET THE `execution-role-arn`
aws iam get-role --role-name emr-serverless-job-role --query 'Role.Arn' --output text


START APPLICATION WITH ALL REQUIRED DETAILS
    --region
    --application-id
    --exeuction-role-arn

aws emr-serverless start-job-run \
    --region us-east-1 \
    --application-id "00fahd28ijaete09" \
    --execution-role-arn arn:aws:iam::618572314333:role/emr-serverless-job-role \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://project-lf/code/test_job.py",
            "sparkSubmitParameters": "--py-files s3://project-lf/code/test_dependency.py 
            --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python 
            --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python 
            
            --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }'
aws emr-serverless start-job-run \
    --application-id $APPLICATION_ID \
    --execution-role-arn $JOB_ROLE_ARN \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://<YOUR_BUCKET>/main.py",
            "sparkSubmitParameters": "--py-files s3://<YOUR_BUCKET>/job_files.zip --conf spark.archives=s3://<YOUR_BUCKET>/pyspark_deps.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }'


https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python-libraries.html

--conf spark.submit.pyFiles=s3://DOC-EXAMPLE-BUCKET/EXAMPLE-PREFIX/<.py|.egg|.zip file>