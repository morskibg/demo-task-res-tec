from .utils import (csv_reader,                    
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
    matched_addresses_df  = create_matching_addresses_by_google_api_df(users_addresses_df)    
    grouped_df = create_grouped_by_address_df(matched_addresses_df, 'place_id')
    csv_file_writer(grouped_df, OUTPUT_FILE_NAME)
    
   