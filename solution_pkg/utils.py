from typing import List,  Dict, Union, Callable, Tuple
import os
import uuid
import re
from dotenv import load_dotenv
import googlemaps
import pandas as pd
from pathlib import Path
import json
from transliterate import translit
from rapidfuzz import fuzz
import googlemaps

from .logger import get_logger
from .constanst import SUB_MAPPERS_FOLDER, FUZZ_TRESHOLD


logger = get_logger(__name__)

load_dotenv()

API_KEY = os.getenv('API_KEY')
googlemaps_client = googlemaps.Client(key = API_KEY)

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
        df.columns = ['name','raw_address']
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

def clean_address_raw_str(
        input_addr: str,
        aggregated_sub_mapper:
            Callable[[str], Union[Dict, Dict[str, str]]] = create_aggr_substitute_mapping(SUB_MAPPERS_FOLDER)) -> str:
    """Clean function called for every input address. Stages:
    1. Split whole address line by ',' and remove leading and trailing white spaces from tokens.
    2. Replace multiple whitespaces inside token with single one.      
    3. Split token by space and tries to find and replace every single splited part with value from mapping dict.

    Args:
        input_addr (str): address
        aggregated_sub_mapper (Callable(str)): Function for creation of aggregated substitute mapper dict. 
                                                Defaults to create_aggr_substitute_mapping(SUB_MAPPERS_FOLDER).

    Returns:
        str: modified address
    """  
     
    tokens = list(map(str.strip, input_addr.split(',')))  
    cleaned = []
    for token in tokens:
        token = translit(re.sub(r'\s{2,}',' ', token.lower()), 'bg', reversed=True)
        substituted =[aggregated_sub_mapper.get(x) if aggregated_sub_mapper.get(x) else x for x in token.split(' ')]
        cleaned.append(" ".join(substituted))
    return ",".join(cleaned)


def create_raw_matching_addresses_df(df: pd.DataFrame, column_name: str = 'cleaned_addr') -> pd.DataFrame:    
    """Create dictionary from all unique preprocessed addresses by comparing one to 
    all others and evaluating is result from 'fuzz.token_set_ratio() exeeds defined treshold. 'token_set_ratio' function tokenize the strings,
    sorts the strings alphabetically, takes out the common tokens and then joins them together. Then, the fuzz.ratio() is calculated.
    Args:
        df (pd.DataFrame): preprocessed input dataframe
        column_name (str): name of the cleaned addresses column 
    Returns:
        pd.DataFrame: preprocessed input dataframe with added column "AddressMapping" for founded similar addresses
    """   
    
    unique_addresses = df[column_name].unique()
    addr_dict = {}
    proceeded_addresses = set()
    for addr in unique_addresses:       
        if addr in proceeded_addresses:
            continue
        for k in list(addr_dict.keys()):            
            if fuzz.token_set_ratio(k, addr) >= FUZZ_TRESHOLD:                             
                addr_dict[k].append(addr)
                break
        else:
            addr_dict[addr] = [addr]

        proceeded_addresses.add(addr)

    inverse_addr_dict = { v: k for k, l in addr_dict.items() for v in l }
    
    def get_value_and_index_by_key(cleaned_addr: str) -> Tuple[str, int]:
        return inverse_addr_dict[cleaned_addr], list(inverse_addr_dict.keys()).index(inverse_addr_dict[cleaned_addr])
    
    df[['raw_mapped_addr','idx']] = df['cleaned_addr'].map(get_value_and_index_by_key).to_list()   
    
    return df 

def create_grouped_by_address_df(df: pd.DataFrame, group_by_name: str = 'place_id', name_str: str = 'name') -> pd.DataFrame:
    """Grouping by 'place_id' column and ordering 

    Args:
        df (pd.Dataframe): dataframe with added similar addresses column
        group_by_name (str): column name to group by

    Returns:
        df (pd.Dataframe): dataframe with users sorted by name and grouped by address
    """    
    grouped_df = (
        df.groupby(group_by_name)[name_str]
            .agg(lambda x: ", ".join(sorted(list(x))))
            .to_frame()
            .sort_values(by = name_str)           
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

def google_maps_address_finder(googlemaps_client: googlemaps.Client, raw_address: str) -> Tuple[str,...]:
    """Searching for address by google's Places API

    Args:
        googlemaps_client (googlemaps.Client): python client for google maps services
        raw_address (str): raw address

    Returns:
        Tuple[str,...]: _description_
    """    
    place_search_result_dict = googlemaps_client.find_place(
        input = raw_address,
        input_type  = "textquery",
        fields = ["formatted_address", "place_id"],
        language = 'en-US',
    ) 
    tupl_to_return = (None, None, place_search_result_dict["status"])    
    
    if place_search_result_dict['status'] == 'OK':
        candidates = place_search_result_dict.get('candidates')        
        if candidates:
            tupl_to_return = (candidates[0]['formatted_address'], candidates[0]['place_id'], place_search_result_dict['status'])
    return tupl_to_return    
    

def create_matching_addresses_by_google_api_df(df:pd.DataFrame, column_name: str = 'raw_address') -> pd.DataFrame:
    """Adding 3 additional columns to initialy read users_addresses dataframe ('formatted_address', 'place_id', 'status')

    Args:
        df (pd.DataFrame): input dataframe read from csv file

    Returns:
        pd.DataFrame: modified users_addresses
    """    
    df = df.copy(deep=False)
    df[['formatted_address', 'place_id', 'status']] = df[column_name].apply(
        lambda x:google_maps_address_finder(googlemaps_client, x)).to_list()
    
    return df
    
    

