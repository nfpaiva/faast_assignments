"""
This module provides a function to clean european life expectancy data files.
"""

from pathlib import Path
import re
import argparse
import pandas as pd


# Get absolute path of this file and its directory path
FILE_PATH = Path(__file__).resolve()
BASE_PATH = FILE_PATH.parent

# Define input and output file paths relative to base path
INPUT_FILE_PATH = BASE_PATH / 'data' / 'eu_life_expectancy_raw.tsv'
OUTPUT_FILE_PATH = BASE_PATH  / 'data' / 'pt_life_expectancy.csv'

# Define variables cleaning  and filtering data
id_vars = ['unit', 'sex', 'age','region']

def clean_data(region_filter: str) -> None:
    """
    This function processes and filters a file with european life expectancy data over the years.

    Parameters:
    None

    Returns:
    Just saves the processed data for Portugal.
    """
    # load data
    df_raw = pd.read_csv(INPUT_FILE_PATH)

    # get name of 3rd column that has region and years data
    col_geo_years = df_raw.columns[3]

    # Split into separate columns region and years
    df_geoyears_cols=df_raw[col_geo_years].str.split('\t', expand=True)
    df_geoyears_cols.columns=df_raw.columns[df_raw.columns.get_loc(col_geo_years)].split('\t')
    df_final=pd.concat([df_raw[['unit','sex','age']],df_geoyears_cols], axis=1)
    df_final.columns.values[df_final.columns.get_loc('geo\\time')]='region'

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_final,id_vars=id_vars, var_name='year')
    # df_final.columns.values[df_final.columns.get_loc('variable')]='year'
    df_final = pd.concat([df_final[df_final['value']!=': '][id_vars + ['year']],
                          df_final[df_final['value']!=': ']['value'].
                          apply(lambda x: re.search(r'\d+(?:\.\d+)?',x).group())]
                          ,axis=1)

    df_final = df_final[(df_final['region']==region_filter)]

    # Save cleaned data to output file
    df_final.to_csv(OUTPUT_FILE_PATH, index=False)

if __name__ == '__main__':# pragma: no cover
    parser = argparse.ArgumentParser(description='Clean European life expectancy data')
    parser.add_argument('--region', type=str, default='PT',
                        help='The region code to filter the data by (default: PT)')
    args = parser.parse_args()

    clean_data(args.region)
