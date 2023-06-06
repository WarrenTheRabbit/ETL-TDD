
from automation.glue import GlueWrapper

class Redshift:
    
    @staticmethod
    def get_copy_command(path):    
        table = GlueWrapper(region_name='ap-southeast-2').create_crawler_path(path).split('/')[-1]
        return f"""COPY {table}
    FROM '{path}'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""