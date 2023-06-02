from typing import Union
from etl.paths.components import Dimension, Fact, Source, Table, Tier, \
    Environment, Load, Bucket
from etl.paths.timestamps import get_current_time_as_timestamp, \
    get_timestamp_for_file, get_lexicographically_highest_subdirectory
from etl.paths.validation import get_validity_of_path_request

def create_path(*,
        # Mandatory keywords.
        environment:Environment, 
        bucket:Bucket,
        tier:Tier, load:Load, 
        time_requested:str,
        # Optional keywords.
        source:Union[Source,str]='',
        file_extension:str='',
        # Mutually exclusive keywords.
        dimension:Union[Dimension,str]='',
        fact:Union[Fact,str]='',
        table:Union[Table,str]='') -> str:
    """
    Returns a valid, timestamped path for a file or folder (either new or 
    existing).
    
    New or existing object?
    -----------------------
    The path can be for a new or existing file or folder, depending on the 
    values for `time_required` and `file_extension`. When `time_requested` is
    'now', the path is for a new object; otherwise, the path is for an existing 
    object (the most recently created). When `file_extension` is an empty 
    string, the new or existing object is a folder; otherwise, it is a file.
    
    Value of timestamp
    ------------------
    The timestamp added to the path depends on `time_required`.
    
    When `time_requested` is 'now', the timestamp takes on the value of the 
    current time.
    
    When `time_requested` is 'recent', the timestamp takes on the value of 
    the result of the look-up, which returns the timestamp for the most
    recently created file/folder that is listed at the intermediary, pre-return
    path.
    
    Look-up behaviour
    -----------------
    The timestamp look-up is only initiated when `time_requested` is 'recent'.
    Once initiated, the behaviour of the look-up depends on `file_extension`. 
    
    When `file_extension` IS an empty string, the look-up checks the timestamps
    of ONLY folders. 
    
    When `file_extension` is NOT an empty string, the look-up checks the 
    timestamps of ONLY files.
    
    Finalising the intermediary path 
    --------------------------------
    The intermediary path is built out of the function's path components, which
    are passed as parameters. Once finalised by adding the timestamp and 
    (in some situations) a file extension, it is returned as the full path.
     
    When `file_extension` is an emptry string, NO file extension is added to 
    the intermediary path. The finalised path is the intermediary path with 
    the timestamp appended as a folder name. 
    
    When `file_extension` is NOT an empty string, the file extension IS added
    to the intermediary path. The finalised path is the intermediary path with 
    the timestamp appended as a file name with extension `file_extension`.
    """    
    # Validate the path request.
    message, validity = get_validity_of_path_request(**locals())
    if validity is False:
        raise ValueError(message)
    
    # Construct the path elements (except for the timestamp, which is 
    # constructed later in the body of the function.
    # 1) Set `structure` to the only provided table type (competition among 
    #    types fails validation).
    # 2) If a `source` has been provided, define a path component (with a 
    #    separator) to include in the prefix; otherwise, include nothing.
    # 3) Build the root.
    # 4) Build the prefix. 
    structure = table or fact or dimension    
    source = (str(source) + "/") if source else ""
    root = f"{environment}://{bucket}"
    prefix = f"{tier}/{source}{structure}/{load}"
    
    # The path is then finalised by 1) adding the requested timestamp (either 
    # the current time or the creation time of the most recently created object
    # (file or folder, depending on `file_extension`) that is listable at the
    # intermediary stem, and 2) by adding `file_extension` (if the path 
    # is for a file).
    if time_requested == 'recent' and not file_extension:
        # The path is intended to locate the most recent folder of parquet 
        # files listable at the stem. Since the folder names are themselves 
        # timestamps, `time_requested` should be set as the lexicographically 
        # higest of the folders.
        timestamp = get_lexicographically_highest_subdirectory(
                                                        bucket=bucket, 
                                                        prefix=prefix)
        return f"{root}/{prefix}/{timestamp}/"
    
    elif time_requested == 'recent' and file_extension:
        # The path is intended to locate the most recent file (with the given
        # extension) that is listable at the stem. Since all file names in the 
        # ETL are timestamps, `time_requested` should be set as the stem of the 
        # most recent file's file name.
        timestamp = get_timestamp_for_file(time_required=time_requested, 
                                           path=f"{root}/{prefix}")
        full_path = f"{root}/{prefix}/{timestamp}{file_extension}"
        return full_path
    elif time_requested == 'now' and not file_extension:
        timestamp = get_current_time_as_timestamp()
   
        full_path = f"{root}/{prefix}/{timestamp}/"
        return full_path
    elif time_requested == 'now' and file_extension:
        timestamp = get_current_time_as_timestamp()
   
        full_path =  f"{root}/{prefix}/{timestamp}{file_extension}"
        return full_path
    else:
        return("ERROR")
    
    
