from typing import List,  Dict, Union, Callable
import os
import re
import pandas as pd
from pathlib import Path
import json
from transliterate import translit
from rapidfuzz import fuzz

from .logger import get_logger
from .constanst import SUB_MAPPERS_FOLDER, FUZZ_WRATIO_TRESHOLD

logger = get_logger(__name__)

def csv_reader(file_name: str) -> pd.DataFrame: 
    """Reading csv file from current working directory and provided file name.

    Args:
        file_name (str): input csv file name

    Raises:
        SystemExit: terminates the program execution

    Returns:
        pandas dataframe: parsed scv data to pandas dataframe
    """    
    try:
        df = pd.read_csv(Path(os.getcwd()).joinpath(file_name))
        return df
    except FileNotFoundError as ex:
        logger.error(f'Missing input csv file.\n{ex}')
        raise SystemExit(2)

def get_data_from_json_file(file_name: str) -> Union[Dict, Dict[str, Union[str, List[str]]]]:
    """Reader for json substitute mapping files

    Args:
        file_name (str): substitution mapper json file name  

    Exception: 
        If json file is missing or corupt - empty dictionary is returned
    Returns:
        Union[Dict, Dict[str, Union[str, List[str]]]]: loaded from json file data
    """    
    data_to_return = {}
    try:
        with open(file_name) as json_file:
            data_to_return = json.load(json_file)
    except (StopIteration, Exception) as ex:
        logger.error(f'Corrupted or empty json mapper - {file_name} - {ex}')
    return data_to_return

def create_aggr_substitute_mapping(mappers_dir: str) -> Union[Dict, Dict[str, str]]:
    """Create single substitution mapping dictionary from all json files in specified directory.
    If data from json files contains list of values for single key - the dictionary is reverted - values becomes keys and vice versa.
    All items are lowered.

    Args:
        mappers_dir (str): Directory name for all single json mapper files

    Returns:
        Union[Dict, Dict[str, str]]: combined dict from all json mappers
    """    
    path = Path(os.getcwd()).joinpath(mappers_dir)
    aggr_mapper = {}
    for _, _, files in os.walk(path):
        for file in files:
            if Path(file).suffix != '.json':                
                continue
            data = get_data_from_json_file(path.joinpath(file))
            if any(isinstance(x, list) for x in data.values()):
                data = { v.lower(): k.lower() for k, l in data.items() for v in l }
            else:
                data = {k.lower():v.lower() for k,v in data.items()}
            aggr_mapper = {**aggr_mapper, **data}
    if not aggr_mapper:
        logger.warning('Empty substitite mapper will be used !')
    return aggr_mapper

def preprocess_address(
        input_addr: str,
        aggregated_sub_mapper:
            Callable[[str], Union[Dict, Dict[str, str]]] = create_aggr_substitute_mapping(SUB_MAPPERS_FOLDER)) -> str:
    """Prepocess function called for every input address. Stages:
    1. Split whole address line by ',' and remove leading and trailing white spaces from tokens.
    2. Replace multiple whitespaces inside token with single one.
    3. Transliterate from cyrillic (bg) to latin (if address is in cirillyc)  
    4. Split token by space and tries to find and replace every single splited part with value from mapping dict.

    Args:
        input_addr (str): address
        aggregated_sub_mapper (Callable(str)): Function for creation of aggregated substitute mapper dict. 
                                                Defaults to create_aggr_substitute_mapping(SUB_MAPPERS_FOLDER).

    Returns:
        str: modified address
    """    
    tokens = list(map(str.strip, input_addr.split(',')))  
    prepocessed = []
    for token in tokens:
        token = translit(re.sub(r'\s{2,}',' ', token.lower()), 'bg', reversed=True)
        substituted =[aggregated_sub_mapper.get(x) if aggregated_sub_mapper.get(x) else x for x in token.split(' ')]
        prepocessed.append(" ".join(substituted))
    return ",".join(prepocessed)


def create_matching_addresses_df(df: pd.DataFrame) -> pd.DataFrame:    
    """Create dictionary from all unique preprocessed addresses by comparing one to 
    all others and evaluating is fuzz WRatio exeeds defined treshold. {
        1. Take the ratio of the two processed strings (fuzz.ratio)
        2. Run checks to compare the length of the strings
        * If one of the strings is more than 1.5 times as long as the other
        use partial_ratio comparisons - scale partial results by 0.9
        (this makes sure only full results can return 100)
        * If one of the strings is over 8 times as long as the other
        instead scale by 0.6
        3. Run the other ratio functions
        * if using partial ratio functions call partial_ratio,
        partial_token_sort_ratio and partial_token_set_ratio
        scale all of these by the ratio based on length
        * otherwise call token_sort_ratio and token_set_ratio
        * all token based comparisons are scaled by 0.95
        (on top of any partial scalars)
        4. Take the highest value from these results
        round it and return it as an integer.
        }
    Args:
        df (pd.DataFrame): preprocessed input dataframe

    Returns:
        pd.DataFrame: preprocessed input dataframe with added column "AddressMapping" for founded similar addresses
    """    
    unique_addresses = df['PreprocAddress'].unique()
    addr_dict = {}
    for addr in unique_addresses:
        if addr in addr_dict or addr in addr_dict.values():
            continue
        for k in list(addr_dict.keys()):
            if fuzz.WRatio(k, addr) >= FUZZ_WRATIO_TRESHOLD:
                addr_dict[k].append(addr)
                break
        else:
            addr_dict[addr] = [addr]

    inverse_addr_dict = { v: k for k, l in addr_dict.items() for v in l }
    df['AddressMapping'] = df['PreprocAddress'].map(inverse_addr_dict)
    return df 

def create_grouped_by_address_df(df: pd.DataFrame) -> pd.DataFrame:
    """Grouping by 'AddressMapping' column and ordering 

    Args:
        df (pd.Dataframe): dataframe with added similar addresses column

    Returns:
        df (pd.Dataframe): dataframe with users sorted by name and grouped by address
    """    
    grouped_df = (
        df.groupby('AddressMapping')['Name']
            .agg(lambda x: ", ".join(sorted(list(x))))
            .to_frame()
            .sort_values(by='Name')           
    )
    return grouped_df

def csv_file_writer(df: pd.DataFrame, file_name: str) -> None:
    """Writing dataframe to csv file

    Args:
        df (pd.DataFrame): dataframe with users sorted and grouped by address
        file_name (str): output csv file name
    """ 
    try:   
        df.to_csv(Path(os.getcwd()).joinpath(file_name), index=False, header=False, sep = ';')
    except Exception as ex:
        logger.error(f'Error saving csv file\n{ex}')