import pytest
from automation.redshift import Redshift

def test_create_copy_statement():
    path = 's3://test-lf-ap/etl/raw/claim_db/claim/full/202306061945/'
    expected_statement = """
    COPY claim
    FROM 's3://test-lf-ap/etl/raw/claim_db/claim/full/202306061945/'
    IAM_ROLE 'arn:aws:iam::618572314333:role/service-role/AmazonRedshift-CommandsAccessRole-20230513T114656'
    FORMAT AS PARQUET;"""
    
    assert Redshift.create_copy_statement(path).strip() == expected_statement.strip()


def test_get_all_redshift_loads():
    test_output = [
        ('df1','s3://test-lf-ap/etl/raw/claim_db/claim/full/202306061945/'),
        ('df2','s3://test-lf-ap/etl/raw/claim_db/policyholder/full/202306061945/'),
        ('df3','s3://test-lf-ap/etl/raw/claim_db/provider/full/202306061945/'),
        ('df4','s3://test-lf-ap/etl/optimised/provider/full/202306061945/')]

    rs = Redshift()
    rs.get_all_redshift_loads(test_output)
    
    assert len(rs.loads) == 1
    assert rs.loads[0][0] == 'df4'
    assert 'optimised' in rs.loads[0][1]


def test_get_copy_commands():
    test_output = [
        ('df1','s3://test-lf-ap/etl/raw/claim_db/claim/full/202306061945/'),
        ('df2','s3://test-lf-ap/etl/raw/claim_db/policyholder/full/202306061945/'),
        ('df3','s3://test-lf-ap/etl/raw/claim_db/provider/full/202306061945/'),
        ('df4','s3://test-lf-ap/etl/optimised/provider/full/202306061945/')]

    rs = Redshift()
    rs.get_all_redshift_loads(test_output)
    commands = rs.get_copy_commands()
    
    assert len(commands) == 1
    assert 'COPY provider' in commands[0]
    assert 'optimised' in commands[0]


def test_get_table_name():
    rs = Redshift()
    assert rs.get_table_name('s3://test-lf-ap/etl/raw/claim_db/claim/full/202306061945/') == 'claim'
    assert rs.get_table_name('s3://test-lf-ap/etl/optimised/provider/full/202306061945/') == 'provider'
