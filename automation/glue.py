# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with AWS Glue to
create and manage crawlers, databases, and jobs.
"""
import boto3
import logging
import time
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
  

class GlueWrapper:
    """Encapsulates AWS Glue actions."""
    def __init__(self, region_name):
        """
        :param glue_client: A Boto3 Glue client.
        """
        self.glue_client = boto3.client('glue', region_name=region_name)
        self.crawler_role = "arn:aws:iam::618572314333:role/service-role/AWSGlueServiceRole-ETL"


    def get_crawler_state(self, path):
        """
        Gets information about a crawler.

        :param name: The name of the crawler to look up.
        :return: Data about the crawler.
        """
        name = self.create_crawler_name(path)
        state = None
        try:
            response = self.glue_client.get_crawler(Name=name)
            state = response['Crawler'].get('State')
        except ClientError as err:
            if err.response['Error']['Code'] == 'EntityNotFoundException':
                logger.info("Crawler %s doesn't exist.", name)
            else:
                logger.error(
                    "Couldn't get crawler %s. Here's why: %s: %s", name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        return state



    def create_crawler(self, paths):
        """
        Creates a crawler.
        
        After creating a crawler, you can use it to populate a database in your 
        AWS Glue Data Catalog with metadata.
        """
        try:
            if isinstance(paths, str):
                path_list = [paths]
            else:
                path_list = paths
                
            s3_targets = [
                {'Path':self.create_crawler_path(path)}
                for path 
                in path_list]
                        
            example_path = s3_targets[-1]['Path']
            name = self.create_crawler_name(example_path)
            db_name = self.create_db_name(example_path)
            table_prefix = self.create_table_prefix(example_path)
            
            response = self.glue_client.create_crawler(
                Name=name,
                Role=self.crawler_role,
                DatabaseName=db_name,
                TablePrefix=table_prefix,
                Targets={'S3Targets': s3_targets})
            
            return response
        except ClientError as err:
            logger.error(
                f"Couldn't create crawler. {err.response['Error']['Message']}")



    def start_crawler(self, path):
        """
        Starts a crawler. The crawler crawls its configured target and creates
        metadata that describes the data it finds in the target data source.

        :param name: The name of the crawler to start.
        """
        try:
            name = self.create_crawler_name(path)
            self.glue_client.start_crawler(Name=name)
        except ClientError as err:
            logger.error(
                f"Couldn't start crawler. {err.response['Error']['Message']}")


    def create_crawler_path(self,path) -> str:
        """
        Returns the path to be crawled by the crawler.
        """
        
        path = path.split('/')
        if 'optimised' in path:
            return '/'.join(path[:path.index('etl')+3]) 
        else:
            return '/'.join(path[:path.index('etl')+4]) 
        
        
       
    
    
    def create_crawler_name(self, path) -> str:
        """
        Get the name for the crawler.
        """
        path = path.split('/')
        bucket = path[2].split('-')[0]
        tier = path[4]
        return f"{bucket}-{tier}"
    
    
    
    def create_db_name(self, path, db_name='release-3') -> str:
        """
        Get the database for the crawler.
        """
        path = path.split('/')
        env = path[2].split('-')[0]
        return f"{env}-{db_name}" 
    
    
    
    def create_table_prefix(self, path):
        """
        Get the prefix for the database.
        """
        path = path.split('/')
        tier = path[4]
        return tier



    def wait_for_crawler(self, path):
        if self.get_crawler_state(path) != 'RUNNING':
            print("Crawler not running.")
            return
        
        print('Waiting for crawler to finish.', end='')
        start = time.time()
        try:
            while self.get_crawler_state(path) == 'RUNNING':
                # Calculate how much time has passed.
                time.sleep(5)
                finish = time.time() - start
                
                # Avoid infinite loop by timing out crawler after 2 minutes.
                if int(finish) > 60 * 4:
                    print()
                    raise Exception('Crawler timed out.')
                else:
                    print('.', end='')
            print()
        except Exception as err:
            logger.error(
                f"Couldn't wait for crawler. 4 minutes elapsed.")
        
        
    def delete_crawler(self, path):
        """
        Deletes a crawler.

        """
        try:
            name = self.create_crawler_name(path)
            self.glue_client.delete_crawler(Name=name)
        except ClientError as err:
            logger.error(
                f"""Couldn't delete crawler:
                {err.response['Error']['Message']}""")
        
    def get_database(self, name):
        """
        Gets information about a database in your Data Catalog.

        :param name: The name of the database to look up.
        :return: Information about the database.
        """
        try:
            response = self.glue_client.get_database(Name=name)
        except ClientError as err:
            logger.error(
                "Couldn't get database %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Database']



    def get_tables(self, db_name):
        """
        Gets a list of tables in a Data Catalog database.

        :param db_name: The name of the database to query.
        :return: The list of tables in the database.
        """
        try:
            response = self.glue_client.get_tables(DatabaseName=db_name)
        except ClientError as err:
            logger.error(
                "Couldn't get tables %s. Here's why: %s: %s", db_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['TableList']



    def create_job(self, name, description, role_arn, script_location, dependencies_s3_uri):
        """
        Creates a job definition for an extract, transform, and load (ETL) job that can
        be run by AWS Glue.

        :param name: The name of the job definition.
        :param description: The description of the job definition.
        :param role_arn: The ARN of an IAM role that grants AWS Glue the permissions
                        it requires to run the job.
        :param script_location: The Amazon S3 URL of a Python ETL script that is run as
                                part of the job. The script defines how the data is
                                transformed.
        :param dependencies_s3_uri: The Amazon S3 URL of a Python dependencies file
                                    that is used by the job.
        """
        try:
            self.glue_client.create_job(
                Name=name, 
                Description=description, 
                Role=role_arn,
                Command={'Name': 'glueetl', 'ScriptLocation': script_location, 'PythonVersion': '3'},
                GlueVersion='3.0',
                DefaultArguments={'--extra-py-files': dependencies_s3_uri}
            )
        except ClientError as err:
            logger.error(
                "Couldn't create job %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise



    def start_job_run(self, name, input_database, input_table, output_bucket_name):
        """
        Starts a job run. A job run extracts data from the source, transforms it,
        and loads it to the output bucket.

        :param name: The name of the job definition.
        :param input_database: The name of the metadata database that contains tables
                               that describe the source data. This is typically created
                               by a crawler.
        :param input_table: The name of the table in the metadata database that
                            describes the source data.
        :param output_bucket_name: The S3 bucket where the output is written.
        :return: The ID of the job run.
        """
        try:
            # The custom Arguments that are passed to this function are used by the
            # Python ETL script to determine the location of input and output data.
            response = self.glue_client.start_job_run(
                JobName=name,
                Arguments={
                    '--input_database': input_database,
                    '--input_table': input_table,
                    '--output_bucket_url': f's3://{output_bucket_name}/'})
        except ClientError as err:
            logger.error(
                "Couldn't start job run %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRunId']

    def list_jobs(self):
        """
        Lists the names of job definitions in your account.

        :return: The list of job definition names.
        """
        try:
            response = self.glue_client.list_jobs()
        except ClientError as err:
            logger.error(
                "Couldn't list jobs. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobNames']



    def get_job_runs(self, job_name):
        """
        Gets information about runs that have been performed for a specific job
        definition.

        :param job_name: The name of the job definition to look up.
        :return: The list of job runs.
        """
        try:
            response = self.glue_client.get_job_runs(JobName=job_name)
        except ClientError as err:
            logger.error(
                "Couldn't get job runs for %s. Here's why: %s: %s", job_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRuns']



    def get_job_run(self, name, run_id):
        """
        Gets information about a single job run.

        :param name: The name of the job definition for the run.
        :param run_id: The ID of the run.
        :return: Information about the run.
        """
        try:
            response = self.glue_client.get_job_run(JobName=name, RunId=run_id)
        except ClientError as err:
            logger.error(
                "Couldn't get job run %s/%s. Here's why: %s: %s", name, run_id,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['JobRun']



    def delete_job(self, job_name):
        """
        Deletes a job definition. This also deletes data about all runs that are
        associated with this job definition.

        :param job_name: The name of the job definition to delete.
        """
        try:
            self.glue_client.delete_job(JobName=job_name)
        except ClientError as err:
            logger.error(
                "Couldn't delete job %s. Here's why: %s: %s", job_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise



    def delete_table(self, db_name, table_name):
        """
        Deletes a table from a metadata database.

        :param db_name: The name of the database that contains the table.
        :param table_name: The name of the table to delete.
        """
        try:
            self.glue_client.delete_table(DatabaseName=db_name, Name=table_name)
        except ClientError as err:
            logger.error(
                "Couldn't delete table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise



    def delete_database(self, name):
        """
        Deletes a metadata database from your Data Catalog.

        :param name: The name of the database to delete.
        """
        try:
            self.glue_client.delete_database(Name=name)
        except ClientError as err:
            logger.error(
                "Couldn't delete database %s. Here's why: %s: %s", name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise