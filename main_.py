import pandas as pd
from utils_ import create_similar_addresses_df, create_final_df, csv_file_writer, csv_reader
from constanst import INPUT_FILE_NAME, OUTPUT_FILE_NAME

def main():
    raw_df = csv_reader(INPUT_FILE_NAME) 
    resul_df = create_similar_addresses_df(raw_df)
    final_df = create_final_df(resul_df)   
    csv_file_writer(final_df, OUTPUT_FILE_NAME)

if __name__ == '__main__':
    main()
    
