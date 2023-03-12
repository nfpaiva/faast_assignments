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

def load_data (input_file_path: Path = INPUT_FILE_PATH) -> pd.DataFrame:
    """
    This function loads a file with european life expectancy data over the years.

    Parameters:
    input_file_path (str): The path to the input file.

    Returns:
    df_raw (pd.DataFrame): The loaded data.
    """
    try:
        # load data
        with open(input_file_path, encoding='utf-8') as filename:
            df_raw = pd.read_csv(filename, sep='\t', na_values=[':'])
    except PermissionError:
        logging.error("Permission denied to read %s.", input_file_path)
        return None
    except FileNotFoundError as error:
        logging.error(str(error))
        return None
    except pd.errors.ParserError as error:
        logging.error("Error reading file %s: %s", input_file_path, error)
        return None
    return df_raw

def __convert_datatypes(dataframe: pd.DataFrame,
                          cols_2_convert: List[str], dtype: str) -> pd.DataFrame:
    """
        This function iterates on each group of columns to convert to the determined data type
    """
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

def clean_data(df_raw: pd.DataFrame, region_filter: str) -> pd.DataFrame:
    """
    This funcion cleans and transforms the data from the raw file.

    Parameters:
    dataframe (pd.DataFrame): The loaded data.
    region_filter (str): The region code to filter the data by.

    Returns:
    df_final (pd.DataFrame): The cleaned data.
    """

    # Split first column into 4 columns
    df_raw[DECOMPOSED_COLs]=df_raw[COMPOSED_COL].str.split(',', expand=True)
    df_raw = df_raw.drop(columns=[COMPOSED_COL])

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_raw,id_vars=DECOMPOSED_COLs, var_name='year')

    #convert column data types explicitly
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

    for dtype, cols in column_groups.items():
        df_final[cols] = __convert_datatypes(df_final.loc[:,cols], cols, dtype)

    df_final = df_final[df_final['region'] ==  region_filter]
    df_final = df_final.dropna(subset=['value'])

    df_final = df_final[(df_final['region']==region_filter)]

    if df_final.empty:# pragma: no cover
        logging.error("No data found for region %s", region_filter)
        return None

    return df_final

def save_data(df_final: pd.DataFrame, output_file_path: str = OUTPUT_FILE_PATH ,
              region_filter: str="") -> None:
    """
    This function saves the cleaned data to a file.

    Parameters:
    df_final (pd.DataFrame): The cleaned data.
    output_file_path (str): The path to the output file.

    Returns:
    None
    """

    try:
        # Save cleaned data to output file
        df_final.to_csv(output_file_path, index=False)
    except PermissionError:# pragma: no cover
        logging.error("Output file %s could not be created or written to.", output_file_path)
        return

    logging.info("Successfully saved data for region %s.",region_filter)

def main(region_filter: str) -> None:
    """
    Function to execute the three main steps of the cleaning process.

    Parameters:
    region (str): The region code to filter the data by.
    
    Returns:
    None

    """
    save_data(
        clean_data(df_raw=
                   load_data(), region_filter=region_filter), region_filter=region_filter)

if __name__ == '__main__':# pragma: no cover
    parser = argparse.ArgumentParser(description='Clean European life expectancy data')
    parser.add_argument('--region', type=str, default='PT',
                        help='The region code to filter the data by (default: PT)')
    args = parser.parse_args()
    main(region_filter = args.region)
