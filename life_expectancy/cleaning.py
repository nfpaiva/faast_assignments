"""
This module provides a function to clean european life expectancy data files.
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)

# Get absolute path of this file and its directory path
FILE_PATH = Path(__file__).resolve()
BASE_PATH = FILE_PATH.parent

# Define input and output file paths relative to base path
INPUT_FILE_PATH = BASE_PATH / "data" / "eu_life_expectancy_raw.tsv"
OUTPUT_FILE_PATH = BASE_PATH / "data" / "pt_life_expectancy.csv"

# Define variables cleaning  and filtering data
COMPOSED_COL = "unit,sex,age,geo\\time"
DECOMPOSED_COLS = ["unit", "sex", "age", "region"]


def load_data(input_file_path: Path = INPUT_FILE_PATH) -> pd.DataFrame:
    """
    This function loads a file with european life expectancy data over the years.

    Parameters:
    input_file_path (Path): The path to the input file.

    Returns:
    pd.DataFrame: The loaded data.
    """

    return pd.read_csv(input_file_path, sep="\t", na_values=[":"], encoding="utf-8")


def __convert_datatypes(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    This function iterates on each group of columns to convert to the determined data type
    """
    # convert column data types explicitly all with exception of "value"
    data_types = {
        "unit": "category",
        "sex": "category",
        "age": "category",
        "region": "category",
        "year": "int",
    }
    try:
        dataframe = dataframe.astype(data_types)
    except ValueError as err:  # pragma: no cover
        logging.error("Datatype conversion error %s", err)
        raise ValueError from err
    # convert value column to numeric
    try:
        dataframe["value"] = pd.to_numeric(
            dataframe["value"].str.extract(r"(\d+(?:\.\d+)?)", expand=False),
            errors="raise",
        )
    except ValueError as err:  # pragma: no cover
        logging.error("Datatype conversion error %s", err)
        raise ValueError from err

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
    df_raw[DECOMPOSED_COLS] = df_raw[COMPOSED_COL].str.split(",", expand=True)
    df_raw = df_raw.drop(columns=[COMPOSED_COL])

    df_final = pd.DataFrame()  # define an empty DataFrame

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_raw, id_vars=DECOMPOSED_COLS, var_name="year")

    # Convert data types
    df_final = __convert_datatypes(df_final)
    df_final = df_final[df_final["region"] == region_filter]
    df_final = df_final.dropna(subset=["value"])

    df_final = df_final[(df_final["region"] == region_filter)]

    if df_final.empty:  # pragma: no cover
        logging.error("No data found for region %s", region_filter)
        return df_final

    return df_final


def save_data(
    df_final: pd.DataFrame,
    output_file_path: Path = OUTPUT_FILE_PATH,
    region_filter: str = "",
) -> None:
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
    except PermissionError as err:  # pragma: no cover
        logging.error(
            "Output file %s could not be created or written to.", output_file_path
        )
        raise PermissionError from err

    logging.info("Successfully saved data for region %s.", region_filter)


def main(region_filter: str) -> None:
    """
    Function to execute the three main steps of the cleaning process.

    Parameters:
    region (str): The region code to filter the data by.

    Returns:
    None

    """
    save_data(
        clean_data(df_raw=load_data(), region_filter=region_filter),
        region_filter=region_filter,
    )


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean European life expectancy data")
    parser.add_argument(
        "--region",
        type=str,
        default="PT",
        help="The region code to filter the data by (default: PT)",
    )
    args = parser.parse_args()
    main(region_filter=args.region)
