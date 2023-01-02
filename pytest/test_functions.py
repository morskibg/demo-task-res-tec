import pandas as pd
import pytest

from solution_pkg.utils import (
    create_aggr_substitute_mapping,
    preprocess_address,
    create_matching_addresses_df,
    create_grouped_by_address_df
    )


def test_create_aggr_substitute_mapping_as_expected():
    test_mappings = create_aggr_substitute_mapping('pytest')  
    excpected_mapping_dict = {
        "test_1_key_1":"test1",
        "test_1_key_2":"test2",
        "test_2_key_1":"1",
        "test_2_key_2":"2"
        }  
    assert test_mappings == excpected_mapping_dict

def test_preprocess_address_latin_as_expected():
     
    mapping_dict = {        
        "ul.":"st"
        }
    test_address_str = "    ul.    Shipka 34, 1000   Sofia     , Bulgaria         "
    expected_addr = "st shipka 34,1000 sofia,bulgaria"    
    assert preprocess_address(test_address_str, mapping_dict) == expected_addr

def test_preprocess_address_cyrillic_as_expected():
     
    mapping_dict = {
        "balgariya":"bulgaria",
        "sofiya":"sofia",        
        "ul.":"st"
        }
    test_address_str = "    ул. Шипка     34,    София,    България         " 
    expected_addr = "st shipka 34,sofia,bulgaria"   
    assert preprocess_address(test_address_str, mapping_dict) == expected_addr 

def test_create_matching_addresses_df_as_expected():
    test_df = pd.DataFrame([
            ["Ilona Ilieva","ул. Шипка 34, София, България","st shipka 34,sofia,bulgaria","st shipka 34,sofia,bulgaria"],
            ["Ivan Draganov","ul. Shipka 34, 1000 Sofia, Bulgaria","st shipka 34,1000 sofia,bulgaria","st shipka 34,sofia,bulgaria"],
            ["Dragan Doichinov","Shipka Street 34, Sofia, Bulgaria","shipka st 34,sofia,bulgaria","st shipka 34,sofia,bulgaria"]
        ],
        columns=['Name','Address','PreprocAddress','AddressMapping']
    )      
    expected_df = create_matching_addresses_df(test_df)    
    try:        
        diff_df = expected_df.compare(test_df) 
        assert diff_df.empty == True
    except ValueError:      
        pytest.fail("'expected_df' and 'test_df' have different columns", False)
   
def test_all_as_expected():
    test_df = pd.DataFrame([
            ["Ivan Draganov","ul. Shipka 34, 1000 Sofia, Bulgaria"],
            ["Leon Wu","1 Guanghua Road, Beijing, China 100020"],            
            ["Ilona Ilieva","ул. Шипка 34, София, България"],
            ["Dragan Doichinov","Shipka Street 34, Sofia, Bulgaria"],
            ["Li Deng","1 Guanghua Road, Chaoyang District, Beijing, P.R.C 100020"],
            ["Frieda Müller","Konrad-Adenauer-Straße 7, 60313 Frankfurt am Main, Germany"]
        ],
        columns=['Name','Address']
    ) 
    expected_df = pd.DataFrame([
        ["Dragan Doichinov, Ilona Ilieva, Ivan Draganov"],
        ["Frieda Müller"],
        ["Leon Wu, Li Deng"]],
        columns=['Name']
    )
    test_df['PreprocAddress'] = test_df['Address'].map(preprocess_address)    
    matched_addresses_df = create_matching_addresses_df(test_df)
    grouped_df = create_grouped_by_address_df(matched_addresses_df)    
    grouped_df.reset_index(inplace = True, drop = True)
    try:        
        diff_df = expected_df.compare(grouped_df) 
        assert diff_df.empty == True
    except ValueError:      
        pytest.fail("'expected_df' and 'test_df' have different columns", False)
    