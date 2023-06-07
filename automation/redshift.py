from automation.glue import Glue
from automation.sns import SNS
from datetime import datetime

class Redshift:
    
    def __init__(self):
        self.loads = []
        self.sns = SNS(subject="There are tables that need to be staged to Redshift.",
                       message="{ }",
                       message_writer = self.get_copy_commands,
                       subject_writer = None)
    

    def create_copy_statement(self, path):    
        table = Glue(region_name='ap-southeast-2').create_crawler_path(path).split('/')[-1]
        return f"""COPY {table}
    FROM '{path}'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""
    
    def get_all_redshift_loads(self, paths):
        # A job in a batch should be loaded into Redshift if it contains 
        # 'optimised' in its path.
        return [path 
                for path
                in paths
                if 'optimised' in path]                
        
    def get_copy_commands(self, paths):
        redshift_sources = self.get_all_redshift_loads(paths)
        return [self.create_copy_statement(path) 
                for path
                in redshift_sources]
    
        

    
    
    