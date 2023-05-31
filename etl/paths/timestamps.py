import boto3
import pytz
import parse
from datetime import datetime
from typing import Optional, Union
from etl.paths.components import Bucket


def get_timestamp_for_file(*, 
                        time_required: str, 
                        path: str) -> str:
    """
    Returns a string representing a timestamp with format YYYYMMDDHHMM.
    
    Typically, the timestamp is either for a new file (in which case, this 
    function will return the current time), or for an existing file (in which 
    case, this function will return the timestamp of the most recently created 
    file in `bucket` that contains `stem` as a subset of its path).
    
    The control flag, `time_required`, tells this function which use case to 
    execute:
        - 'new' if a new timestamp is required (see Use Case 1).
        - 'recent' if the timestamp of the most recently created file 
          containing the given address (`bucket`/`stem`) as a subpath is 
          required (see Use Case 2).
           
    Parameters:
    -----------
        `time_required` (str): A string that is either 'now' or 'recent'.
        `path` (str): The path at which to get all listable file objects.
        
    Returns:
    --------
        str: A string representing a timestamp with format YYYYMMDDHHMM.
        
    Raises:
    -------
        ValueError: If the `time_required` control flag is not 'now' or 
        'recent'.
    """       
    # Validate the `time_required` control flag.
    if time_required not in ['now', 'recent']:
        raise ValueError(f"Invalid `time_required` flag: {time_required}")
    
    if time_required == 'now':
        # The user is requesting the current time. Calculate the timestamp and
        # return it.
        timestamp = get_current_time_as_timestamp()
        return timestamp
    else:           
        # The user is requesting the timestamp for the most recently created 
        # object with the subpath `bucket`/`stem`. Calculate the timestamp and 
        # return it.       
        timestamp = get_timestamp_of_most_recently_created_file(path)
        return timestamp
    
    
def get_timestamp_of_most_recently_created_file(path:str) -> str:
    """Return the timestamp of the most recently created file that is 
    listable at the provided path.

    Args:
        bucket (Bucket): The name of the bucket to search.
        stem (str): The key of the file objects to search for.

    Returns:
        str: The most recent date in the bucket.

    Raises:
        ValueError: If no files are found in the bucket.
        ValueError: If no dates are found in the bucket.
    """

    bucket, prefix = parse.parse("s3://{}/{}", path)
    bucket = boto3.resource('s3').Bucket(bucket)
     
    file_paths = [obj for obj in bucket.objects.filter(Prefix=prefix)]
    if not file_paths:
        raise ValueError("No files found in bucket.")

    dates = []
    for file in file_paths:
        date_string = file.key.split('/')[-1].split('.')[0]
        if date_string:
            dates.append(date_string)

    if not dates:
        raise ValueError("No recent dates found in bucket.")

    return dates[-1]

def get_current_time_as_timestamp():
    return (datetime
            .now(tz=pytz.timezone('Australia/Sydney'))
            .strftime("%Y%m%d%H%M"))
    
def get_lexicographically_highest_subdirectory(bucket, prefix) -> str:
    """
    List all subdirectories under a prefix in an S3 bucket and return the 
    lexicographically highest.

    Parameters:
        bucket (str): The name of the S3 bucket.
        prefix (str): The prefix (i.e., "directory path") to search under.

    Returns:
        str: The lexicographically highest subdirectory under the prefix.
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(str(bucket))
    
    print(f"get_lexicographically_highest_subdirectory() is exploring {bucket}/{prefix}")
    
    candidate_dates = []
    for obj in bucket.objects.filter(Prefix=prefix):
        subdirs = obj.key.split('/')
        leaf = subdirs[-2:][0]
        if leaf.isdigit():
            candidate_dates.append(leaf)
    try:
        most_recent = sorted(candidate_dates)[-1]
        print(f"\tMost recent found: {most_recent}")
        return most_recent
    except IndexError:
        print(f"No folder of parquet files found at {bucket}/{prefix}")
        return ""
    
    
    
    """Usage Notes:
    ------------
        
    Use Case 1.
    If the user calls this function with 'now' as the `time_required` argument, 
    the function will return the current time. 
    
    >>> timestamp = get_timestamp_for_file(time_required='now', 
    ...                 bucket_name='landing', 
    ...                 stem='claim_db/claim/')
    202305241200
    # Assuming it is currently 2023/05/24 12:00.)
    
    Use Case 2. 
    If the user calls this function with 'recent' as the 
    `time_required` argument, this function will delegate the task of searching 
    for the most recent file among similarly addressed files and returning its 
    timestamp. 
    
    >>> get_timestamp_for_file(time_required='recent', 
    ...         bucket_name='landing', 
    ...         stem='claim_db/claim/') 
    202301010000
    # Assuming 'landing/claim_db/claim/' contains 
    #            202201010000.csv
    #            202301010000.csv                
    
    Currently (2023/05/24), this function is called by the `create_path` 
    function, which has the job of creating a timestamped addresss for use in 
    read and write ETL operations. Although timestamping operations has general
    utility, obtaining timestamps is doubly useful in `create_path`'s context, 
    as the ETL system it serves employs the convention of naming files with the 
    time of their creation. 
    
    A typical scenario is for `create_path` to call this function when the ETL 
    pipeline needs an address from which to read the most recently staged 
    database table (in the form of a .csv file, for example). For this use case, 
    `time_required` is set to 'now'. Another use case is when an ETL process 
    needs an address to write new parquet data to. For this use case, 
    `time_required` is set to 'now'."""