""" This module is the main module of the life_expectancy package. 
It is responsible for executing the 3 steps - loading, cleaning and saving"""

from pathlib import Path
import argparse
import pandas as pd
from life_expectancy.file_handler import (
    FileHandler,
    CSVFileLoadingStrategy,
    JSONFileLoadingStrategy,
    ZipFileLoadingStrategy,
)
from life_expectancy.data_cleaning import (
    DataCleaner,
    CSVCleaningStrategy,
    JSONCleaningStrategy,
)
from life_expectancy.region import Region


def loading_cleaning_saving(
    country: Region, input_file: Path, input_file_ext: str
) -> pd.DataFrame:
    """
    loading_cleaning_saving function responsible for executing the 3 steps -
    loading, cleaning and saving
    """
    # Get absolute path of this file and its directory path
    FILE_PATH = Path(__file__).resolve()
    BASE_PATH = FILE_PATH.parent

    # Define output file path relative to base path
    OUTPUT_FILE_PATH = (
        BASE_PATH / "data" / f"{str(country.value).lower()}_life_expectancy.csv"
    )

    # Choose the appropriate strategy based on the input file type
    if input_file_ext in (".csv", ".tsv"):
        filehandler = FileHandler(CSVFileLoadingStrategy())
        # Define variables cleaning  and filtering data
        composed_col = "unit,sex,age,geo\\time"
        decomposed_cols = ["unit", "sex", "age", "region"]
        cleaner = DataCleaner(CSVCleaningStrategy(composed_col, decomposed_cols))
    elif input_file_ext == ".json":
        filehandler = FileHandler(JSONFileLoadingStrategy())
        cleaner = DataCleaner(JSONCleaningStrategy())
    elif input_file_ext == ".zip":
        filehandler = FileHandler(ZipFileLoadingStrategy())
        cleaner = DataCleaner(JSONCleaningStrategy())
    else:
        raise ValueError(f"Unsupported file type: {input_file_ext}")

    df_raw = filehandler.load_data(input_file)

    df_final = cleaner.clean_data(df_raw, country)

    filehandler.save_data(df_final, OUTPUT_FILE_PATH, country.value)

    return df_final


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description="Clean European life expectancy data")
    parser.add_argument(
        "--region",
        type=Region,
        default=Region.PT,
        help="The region code to filter the data by (default: PT)",
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        default=None,
        help="The input file containing life expectancy data (default: eu_life_expectancy_raw.tsv)",
    )
    args = parser.parse_args()

    # If input file is not provided, use the default input file path
    if args.input_file is None:
        FILE_PATH_DEFAULT = Path(__file__).resolve()
        BASE_PATH_DEFAULT = FILE_PATH_DEFAULT.parent
        args.input_file = BASE_PATH_DEFAULT / "data" / "eu_life_expectancy_raw.tsv"

    # Get the file extension of the input file
    file_ext = args.input_file.suffix.lower()

    loading_cleaning_saving(args.region, args.input_file, file_ext)
