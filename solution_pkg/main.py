import pandas as pd
from .utils import (csv_reader, 
                    clean_address_raw_str,    
                    create_raw_matching_addresses_df,               
                    create_grouped_by_address_df,
                    csv_file_writer,
                    create_matching_addresses_by_google_api_df
                    )
from .constanst import INPUT_FILE_NAME, OUTPUT_FILE_NAME
from .logger import get_logger
from .decorator import timed

logger = get_logger(__name__)

@timed
def main():
    users_addresses_df = csv_reader(INPUT_FILE_NAME)     
    users_addresses_df['cleaned_addr'] = users_addresses_df['raw_address'].map(clean_address_raw_str)    
    raw_matched_addresses_df = create_raw_matching_addresses_df(users_addresses_df)    
    unique_addr_df = raw_matched_addresses_df.drop_duplicates(subset = ['idx'], keep = 'first')    
    api_matched_addresses_df = create_matching_addresses_by_google_api_df(unique_addr_df)    
    final_df = pd.merge(users_addresses_df, api_matched_addresses_df[['idx', 'formatted_address', 'place_id','status']], how = 'outer', on = 'idx') 
    grouped_df = create_grouped_by_address_df(final_df)
    csv_file_writer(grouped_df, OUTPUT_FILE_NAME)
    
   