from .utils import (csv_reader,
                    preprocess_address, create_matching_addresses_df,
                    create_grouped_by_address_df,
                    csv_file_writer
                    )
from .constanst import INPUT_FILE_NAME, OUTPUT_FILE_NAME
from .logger import get_logger
from .decorator import timed

logger = get_logger(__name__)

@timed
def main():
    users_addresses_df = csv_reader(INPUT_FILE_NAME)     
    users_addresses_df['PreprocAddress'] = users_addresses_df['Address'].map(preprocess_address)    
    matched_addresses_df = create_matching_addresses_df(users_addresses_df)
    grouped_df = create_grouped_by_address_df(matched_addresses_df)
    print(grouped_df)
    csv_file_writer(grouped_df, OUTPUT_FILE_NAME)

