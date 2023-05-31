"""
This module defines a descriptive and value-safe abstraction to be used when
creating the path names needed by the pipeline's read and write operations. 

The basic use case is to pass appropriate keyword-only enums to the `create_path` 
method. The correct combination of keywords to pass depends on where and how
the ETL operation is situated in the pipeline. For example, ETL operations can 
vary by environment (development or production), staging tier (Landing, Raw, 
Access or Optimised), source-system orientation (from operational databases or 
integrative), table (dimension or non-dimensional), load type (full or 
incremental), timestamp and file extension.

A more advanced use case is to toggle the environment. If the Environment.PROD 
value is used to start a path, the ETL processes will interact with S3 file 
paths. If the Environment.DEV value is used, the ETL processes will interact 
with a local file system instead. 

Usage Examples:

    A development environment demonstration.
    >>> path = (
    ...     f"{Environment.DEV}"
    ...     f"{Bucket.LANDING}"
    ...     f"{Source.CLAIM_DB}"
    ...     f"{Table.CLAIM}"
    ...     f"claim.csv"
    ... )
    >>> path
    's3://landing/claim_db/claim/claim.csv'
   
    A production environment demonstration.
    >>> path = (
    ...     f"{Environment.PROD}"
    ...     f"{Bucket.LANDING}"
    ...     f"{Source.CLAIM_DB}"
    ...     f"{Table.CLAIM}"
    ...     f"claim.csv"
    ... )
    >>> path
    's3://landing/claim_db/claim/claim.csv'
"""
from enum import Enum
from typing import Iterable, Union

class StringEnum(Enum):
    """A convenience class that provides its subclasses with a __str__ method
    that returns the value of the enum as a string; this is useful for string
    formatting operations."""
    
    def __str__(self):
        return self.value    


class Bucket(StringEnum):
    """The path component used by the ETL pipeline in production to reference
    the S3 bucket."""
    
    LANDING = "landing-dev-wm"
    RAW = "project-dev-wm" 
    ACCESS = "project-dev-wm"
    OPTIMISED = "project-dev-wm"
    TEST = "test-dev-wm"
    PROJECT = "project-dev-wm"
    
class Tier(StringEnum):
    """"""
    LANDING = "landing"
    RAW = "raw" 
    ACCESS = "access"
    OPTIMISED = "optimised"
    
class Source(StringEnum):
    """The path component used by the ETL pipeline when a process is closely
    related to a specific source system."""
    
    CLAIM_DB = "claim_db"
    
    
class Table(StringEnum):
    """The path component used by the ETL pipeline when a process is closely
    related to a specific table from a source system."""
    
    CLAIM = "claim"
    PROVIDER = "provider"
    POLICYHOLDER = "policyholder"
    
    
class Fact(StringEnum):
    """The path component used by the ETL pipeline when a process is closely
    related to a specific fact table."""
    
    CLAIM = "claim_fact"
        
        
class Dimension(StringEnum):
    """The path component used by the ETL pipeline when a process is closely
    related to a specific dimension table."""
    
    DATE = 'date_dim'
    LOCATION = 'location_dim'
    POLICYHOLDER = 'policyholder_dim'
    PROCEDURE = 'procedure_dim'
    PROVIDER = 'provider_dim'
    
    
class Load(StringEnum):
    """The path componet used by the ETL pipeline when a process relates to
    a specific load type."""
    
    FULL = 'full'
    INCREMENTAL = 'incremental'
    
    
class Environment(StringEnum):
    """The path component used by the ETL pipeline when a process relates to
    a specific environment type."""
    
    PROD = "s3"
    DEV = 'file'
    
import boto3
import boto3.resources.base as ServiceResource
import pytz
from datetime import datetime

session = boto3.Session()
s3 = session.resource('s3')
    







    
# When run as a module, the Usage Notes are executed as tests.
if __name__ == '__main__':
    import doctest
    doctest.testmod()