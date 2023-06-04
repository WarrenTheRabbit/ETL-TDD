import boto3
import json
import time

class DataBrew():
    """
    A class to interact with AWS DataBrew.
    """
    
    def __init__(self, region_name):
        self.client = boto3.client('databrew', region_name=region_name)
        
    
    def create_dataset(self, path, database='test-release-3', CatalogId='618572314333'):
        """
        A dataset needs a name for itself and a reference to a unique table in the data 
        catalog.
        
        After a dataset has been created, it can be used to create a profile job.
        """
        # Define the catalog configuration
        input_config = {
            'DataCatalogInputDefinition': 
                {
                    'CatalogId': '618572314333',
                    'DatabaseName': database,
                    'TableName': self.create_table_name(path)
                }
        }
        
        response = self.client.create_dataset(
            Name=self.create_dataset_name(path),
            Input=input_config
        )

        return response


    def create_profile_job(self, path, key='dataquality'):
        """
        A Databrew profile job needs a dataset to run on, a name, and an output location.
        
        After the job has been created, it can be run using its name.
        """
        output_config = {
                'Bucket': self.get_bucket_name(path),
                'Key': key
        }
        
        response = self.client.create_profile_job(
            DatasetName=self.create_dataset_name(path),
            Name=self.create_job_name(path),
            OutputLocation=output_config,
            RoleArn='arn:aws:iam::618572314333:role/DataBrew-Data-Quality-Workflows-DataAccessRole-ZRAZ5RGWCQW0'
        )
        
        return response
        
        
    def start_job_run(self, path):
        """
        Start a job run for a given job name.
        
        After the job run has completed, the results can be retrieved from the
        S3 bucket.
        """
        response = self.client.start_job_run(
            Name=self.create_job_name(path)
        )
        return response
    
    
    def get_job_state(self, path):
        """
        Return the state of a given job name.
        """
        response = self.client.list_job_runs(Name=self.create_job_name(path))
        result = response['JobRuns'][0]['State']
        return result
        
        
    def delete_job(self, path):
        """
        Delete a job.
        """
        response = self.client.delete_job(Name=self.create_job_name(path))
        return response
        
    
    def wait_for_job(self, job_name):
        if self.get_job_state(job_name) != 'RUNNING':
            # print("Job is", self.get_job_state(job_name))
            return
        
        print('Waiting for job to finish.', end='')
        start = time.time()
        while self.get_job_state(job_name) == 'RUNNING':
            time.sleep(5)
            finish = time.time() - start
            
            if int(finish) > 300:
                print()
                raise Exception('Job timed out')
            else:
                print('.', end='')
        

    
    def get_path_to_profile_file(self,path, key='dataquality'):
        """
        Get the profile object for a given job name.
        
        >>> get_profile_object_path(job_name=f"profile-test-raw-claim")
        ('test-lf-wm','dataquality/test-raw-claim_e8f23d26addbdb1da6258f6a086e99bd3724deded967ad1deef4682849b04386.json')
        
        """
        
        latest_run = self.client.list_job_runs(Name=self.create_job_name(path))['JobRuns'][0]
        # Get location information.
        location = latest_run['Outputs'][0]['Location']
        # Get path
        bucket, key = location['Bucket'], location['Key']
        result = bucket, key
        # print("get_profile_path:", result)
        return bucket,key
    
    
    def get_profile_link(self,path):
        """
        Return the link to the profile page for a given dataset.
        """
        dataset = self.create_dataset_name(path)
        return f"https://us-east-1.console.aws.amazon.com/databrew/home?region=us-east-1#dataset-details?dataset={dataset}&tab=profile-overview"
    
    

    def get_dq_results(self, path):
        """
        Get the data quality results from the S3 bucket.
        
        The data quality results can be used to validate a transformation.
        """
        
        bucket,key = self.get_path_to_profile_file(path)
        
        s3_resource = boto3.resource('s3', region_name='us-east-1')
        s3_object = s3_resource.Object(bucket, key)
        dq_results = s3_object.get()["Body"].read().decode("utf-8")
        dict_results = json.loads(dq_results)

        return dict_results
    
    # -------------------------------------------------------------------------
    # DATABREW EXTRACTING INFORMATION UTILITIES
    # -------------------------------------------------------------------------
    def create_dataset_name(self, path):
        path = path.replace('_', '-').split("/")
        env = path[2].split('-')[0]
        identifier = '-'.join([path[4],path[6]])
        result = f"{env}-{identifier}"
        
        # print("create_dataset_name", result)
        return result

    def get_bucket_name(self,path):
        """
        Get the bucket name in the path 
        """
        path = path.split("/")
        bucket = path[2]
        
        result = bucket   
        # print("create_table_name:", result)
        return result
    
    
    def create_table_name(self,path):
        """
        Create the table name for the 
        """
        path = path.split("/")
        tier = path[4]
        table = path[6]
        
        result = f"{tier}{table}"   
        # print("create_table_name:", result)
        return result
    
    
    def create_job_name(self,path):
        """
        Create the job name for the DataBrew profile job.
        """
        path = path.split("/")
        env = path[2].split('-')[0]
        tier = path[4]
        table = path[6]
  
        result = f"{env}-{tier}-{table}"   
        # print("create_job_name():", result)
        return result

    
   
