""" Main module for the life_expectancy package """

from pathlib import Path
import argparse
import pandas as pd
from life_expectancy.file_handler import FileHandler
from life_expectancy.data_cleaning import DataCleaner


def loading_cleaning_saving(country: str) -> pd.DataFrame:
    """
    loading_cleaning_saving function responsible for executing the 3 steps -
    loading, cleaning and saving
    """

    # Get absolute path of this file and its directory path
    FILE_PATH = Path(__file__).resolve()
    BASE_PATH = FILE_PATH.parent

    # Define input and output file paths relative to base path
    INPUT_FILE_PATH = BASE_PATH / "data" / "eu_life_expectancy_raw.tsv"
    OUTPUT_FILE_PATH = BASE_PATH / "data" / "pt_life_expectancy.csv"

    filehandler = FileHandler()
    df_raw = filehandler.load_data(INPUT_FILE_PATH)

    cleaner = DataCleaner()
    df_final = cleaner.clean_data(df_raw, country)

    filehandler.save_data(df_final, OUTPUT_FILE_PATH, country)

    return df_final


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean European life expectancy data")
    parser.add_argument(
        "--region",
        type=str,
        default="PT",
        help="The region code to filter the data by (default: PT)",
    )
    args = parser.parse_args()

    loading_cleaning_saving(args.region)
