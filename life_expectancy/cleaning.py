"""
This module provides a function to clean european life expectancy data files.
"""

import re
import argparse
import pandas as pd

# Define input and output file paths
INPUT_FILE_PATH = './life_expectancy/data/eu_life_expectancy_raw.tsv'
OUTPUT_FILE_PATH = './life_expectancy/data/pt_life_expectancy.csv'

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

    # Split column that has region and years data into separate columns
    df_geoyears_cols=df_raw.iloc[:,3].str.split('\t', expand=True)
    df_geoyears_cols.columns=df_raw.columns[3].split('\t')
    df_final=pd.concat([df_raw.iloc[:,0:3],df_geoyears_cols], axis=1)
    df_final.columns.values[3]='region'

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_final,id_vars=id_vars)
    df_final = pd.concat([df_final[df_final['value']!=': '].iloc[:,:5],
        df_final[df_final['value']!=': '].iloc[:,5].
            apply(lambda x: re.search(r'\d+(?:\.\d+)?',x).group())], axis=1)
    df_final.columns.values[4]='year'
    df_final = df_final[(df_final['region']==region_filter)]

    # Save cleaned data to output file
    df_final.to_csv(OUTPUT_FILE_PATH, index=False)

if __name__ == '__main__':# pragma: no cover
    parser = argparse.ArgumentParser(description='Clean European life expectancy data')
    parser.add_argument('--region', type=str, default='PT',
                        help='The region code to filter the data by (default: PT)')
    args = parser.parse_args()

    clean_data(args.region)
