"""
This module provides a function to clean european life expectancy data files.
"""

from pathlib import Path
from typing import List
import argparse
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get absolute path of this file and its directory path
FILE_PATH = Path(__file__).resolve()
BASE_PATH = FILE_PATH.parent

# Define input and output file paths relative to base path
INPUT_FILE_PATH = BASE_PATH / 'data' / 'eu_life_expectancy_raw.tsv'
OUTPUT_FILE_PATH = BASE_PATH  / 'data' / 'pt_life_expectancy.csv'

# Define variables cleaning  and filtering data
COMPOSED_COL = 'unit,sex,age,geo\\time'
DECOMPOSED_COLs = ['unit', 'sex', 'age','region']

def clean_data(region_filter: str) -> None:
    """
    This function processes and filters a file with european life expectancy data over the years.

    Parameters:
    None

    Returns:
    Just saves the processed data for Portugal.
    """

    try:
        # load data
        with open(INPUT_FILE_PATH, encoding='utf-8') as filename:
            df_raw = pd.read_csv(filename, sep='\t', na_values=[':'])
    except FileNotFoundError:# pragma: no cover
        logging.error("Input file %s not found.", INPUT_FILE_PATH)
        return

    # Split first column into 4 columns
    df_raw[DECOMPOSED_COLs]=df_raw[COMPOSED_COL].str.split(',', expand=True)
    df_raw = df_raw.drop(columns=[COMPOSED_COL])

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_raw,id_vars=DECOMPOSED_COLs, var_name='year')

    data_types = {
        'unit': 'str',
        'sex': 'str',
        'age': 'str',
        'region': 'str',
        'year': 'int',
        'value': 'float64',
    }

    column_groups = {value: [key for key, val in data_types.items() if val == value]
                 for value in set(data_types.values())}

    def convert_datatypes(dataframe: pd.DataFrame,
                          cols_2_convert: List[str], dtype: str) -> pd.DataFrame:
        for col in cols_2_convert:
            if cols_2_convert == ['value']:
                try:
                    dataframe[col] = pd.to_numeric(dataframe[col]\
                                                   .str.extract(r'(\d+(?:\.\d+)?)',
                                                                expand=False), errors='raise')
                except ValueError as error:
                    logging.error("Datatype conversion error %s", error)
            else:
                try:
                    dataframe[col] = dataframe[col].astype(dtype=dtype, errors='raise')
                except ValueError as error:
                    logging.error("Datatype conversion error %s", error)
        return dataframe

    for dtype, cols in column_groups.items():
        df_final[cols] = convert_datatypes(df_final.loc[:,cols], cols, dtype)

    df_final = df_final.query("region == @region_filter and value.notna()")

    df_final = df_final[(df_final['region']==region_filter)]

    if df_final.empty:# pragma: no cover
        logging.error("No data found for region %s", region_filter)
        return

    try:
        # Save cleaned data to output file
        df_final.to_csv(OUTPUT_FILE_PATH, index=False)
    except PermissionError:# pragma: no cover
        logging.error("Output file %s could not be created or written to.", OUTPUT_FILE_PATH)
        return

    logging.info("Successfully cleaned data for region %s.",region_filter)


if __name__ == '__main__':# pragma: no cover
    parser = argparse.ArgumentParser(description='Clean European life expectancy data')
    parser.add_argument('--region', type=str, default='PT',
                        help='The region code to filter the data by (default: PT)')
    args = parser.parse_args()

    clean_data(args.region)
