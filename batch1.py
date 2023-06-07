import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job



from automation.batch import Batch
from automation.redshift import Redshift
from automation.glue import Glue
from automation.databrew import DataBrew

from etl.paths.components import Bucket
from etl.batches import batch1

# Initialise classes to automate interactions with AWS services.
redshift = Redshift()
glue = Glue(region_name='ap-southeast-2')
databrew = DataBrew(region_name='ap-southeast-2')

def main(**config):
    """
    This function is the entry point for the AWS Glue job.
    """
    # Run the current batch.
    batch = batch1.get_batch(**config)
    batch.run()
    
    # Use Glue to crawl and catalog all S3 locations in output.
    glue.catalog_S3_locations(batch.paths)
    
    # Use Databrew to profile the cataloged data at each S3 location.
    databrew.profile_S3_locations(batch.paths)
    
    # Send SNS emails.
    for path in batch.paths:
        # Send a link to data profile.
        databrew.sns.publish(path)
       
        # Send SQL statement if data needs to be staged to Redshift.
        redshift.sns.publish(path)
   
    
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