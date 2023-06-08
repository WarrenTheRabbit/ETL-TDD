from typing import Union, Optional
from paths.components import Dimension, Fact, Source, Table, Tier
from collections import namedtuple

# Define a data structure with error_message and validity fields.
ValidationResult = namedtuple('ValidationInformation',
                              ['error_message', 'validity'])

def get_validity_of_path_request(**kwargs) -> ValidationResult:
    """
    Returns True if the path request is valid; otherwise, return False.
    """
    # Extract arguments required for validation.
    dimension:Union[Dimension,str] = kwargs.get('dimension','')
    fact:Union[Fact,str] = kwargs.get('fact','')
    file_extension:str = kwargs.get('file_extension','')
    source:Union[Source,str] = kwargs.get('source', '')
    table:Union[Table,str] = kwargs.get('table','')
    tier:Union[Tier,str] = kwargs.get('tier','')
    
    # Test arguments against the validation rules. The path is valid when no 
    # validation sentinels are triggered.
    if not only_one_table_type_per_path(fact, dimension, table):
        message = "Only one table type can be requested per path."
        return ValidationResult(message, False)
        
    if not only_landing_tier_paths_have_extensions(tier, file_extension):
        message = "Only paths in the Landing Tier can have file extensions."
        return ValidationResult(message, False)
    
    if not only_nonanalytical_table_paths_have_sources(fact,dimension,source):
        message = "Only paths for non-analytical tables can have sources."
        return ValidationResult(message,False)
    
    return ValidationResult("Path valid.", True)

def only_one_table_type_per_path(fact:Union[Fact,str], 
                                 dimension:Union[Dimension,str], 
                                 table:Union[Table,str]) -> bool:
    """
    Validate that the path is for exactly one structure.
    """
    number_of_requested_structures = sum([bool(structure) 
                                     for structure
                                     in [fact, dimension, table]])
    is_exactly_one_structure_requested = (number_of_requested_structures == 1)
    
    if is_exactly_one_structure_requested:
        return True
    else:
        return False
    
def only_landing_tier_paths_have_extensions(tier:Union[Tier,str], 
                                            file_extension:str) -> bool:
    """
    Return False if the path is for a file outside the landing tier;
    otherwise, return True.
    """
    is_in_landing_tier = (tier == Tier.LANDING)
    is_file = bool(file_extension)
    
    if (is_file and not is_in_landing_tier):
        return False
    else: 
        return True
    
def only_nonanalytical_table_paths_have_sources(fact:Union[Fact,str], 
                                                dimension:Union[Dimension,str],
                                                source:Union[Source,str]) -> bool:
    """
    Return False if the path is for an analytical table and includes a source
    component; otherwise, return True.
    """    
    analytical_path_requested:bool = bool(fact or dimension)  
    source_history_requested:bool = bool(source)

    if (analytical_path_requested and source_history_requested):
        return False
    else:
        return True