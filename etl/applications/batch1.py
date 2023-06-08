import logging
import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from aws.redshift import Redshift
from aws.glue import Glue
from aws.databrew import DataBrew

from paths.components import Bucket
from etl.batch import batch1 as batch_definition

# Initialise classes to automate interactions with AWS services.
redshift = Redshift()
glue = Glue(region_name='ap-southeast-2')
databrew = DataBrew(region_name='ap-southeast-2')

def main(**config):
    """
    This function is the entry point for the AWS Glue job.
    """
    # Run the current batch.
    batch =  batch_definition.get_batch(**config)
    batch.run()
    
    # Use Glue to crawl and catalog all S3 locations in output.
    glue.catalog_S3_locations(batch.paths)
    
    # Use Databrew to profile the cataloged data at each S3 location.
    databrew.profile_S3_locations(batch.paths)
        
    # Send a notification for each data profile that was created.
    for path in batch.paths:
        databrew.sns.publish(path)
    
    # If any data needs to be staged to Redshift, send DDL statements as a
    # notification.
    redshift.sns.publish(batch.paths)
   
    
if __name__ == "__main__":
    
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    config = {
        'spark': spark,
        'env': Bucket.PROD
    }
    
    job.init(args['JOB_NAME'], args)
    main(**config)
    job.commit()
    
    logging.info("Batch completed.")