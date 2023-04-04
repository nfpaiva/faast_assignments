"""
This module provides functions to load and save files.
"""

from pathlib import Path
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)


def load_data(input_file_path: Path) -> pd.DataFrame:
    """
    This function loads a raw file with european life expectancy data over the years.
    """
    try:
        # load data
        with open(input_file_path, encoding="utf-8") as filename:
            df_raw = pd.read_csv(filename, sep="\t", na_values=[":"])
    except FileNotFoundError:  # pragma: no cover
        logging.error("Input file %s not found.", input_file_path)
        return pd.DataFrame()
    logging.info("Successfully loaded %s", input_file_path)
    return df_raw


def save_data(
    df_final: pd.DataFrame, output_file_path: Path, region_filter: str
) -> None:
    """
    This function saves a cleaned and filtered version of
    original european life expectancy data over the years.
    """
    try:
        # Save cleaned data to output file
        df_final.to_csv(output_file_path, index=False)
    except PermissionError:  # pragma: no cover
        logging.error(
            "Output file %s could not be created or written to.", output_file_path
        )
        return

    logging.info(
        "Successfully saved cleaned data for region %s at %s",
        region_filter,
        output_file_path,
    )
