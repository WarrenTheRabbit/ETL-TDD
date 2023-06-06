
from typing import List
from automation.glue import GlueWrapper

class Redshift:
    
    def __init__(self):
        self.loads = []
    
    @staticmethod
    def create_copy_statement(path):    
        table = GlueWrapper(region_name='ap-southeast-2').create_crawler_path(path).split('/')[-1]
        return f"""COPY {table}
    FROM '{path}'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""
    
    def get_any_redshift_loads(self, batch:List):
        # A job in a batch should be loaded into Redshift if it contains 
        # 'optimised' in its path.
        self.loads += [(df,path) 
                       for (df,path) 
                       in batch 
                       if 'optimised' in path]                
        
    def get_copy_commands(self):
        return [Redshift.create_copy_statement(path) 
                for (_,path)
                in self.loads]