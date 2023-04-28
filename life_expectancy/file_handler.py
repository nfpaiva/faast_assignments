"""
This module provides a class to load and save files.
"""

from pathlib import Path
import logging
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)


class FileHandler:
    """This class provides methods to load and save files."""

    def __init__(self):
        self.output_file_path = None
        self.region_filter = None

    def save_data(
        self, df_final: pd.DataFrame, output_file_path: Path, region_filter: str
    ) -> None:
        """
        This method saves a cleaned and filtered version of
        original european life expectancy data over the years.
        """
        self.output_file_path = output_file_path
        self.region_filter = region_filter

        try:
            # Save cleaned data to output file
            df_final.to_csv(self.output_file_path, index=False)
        except PermissionError:
            logging.error(
                "Output file %s could not be created or written to.",
                self.output_file_path,
            )
            return

        logging.info(
            "Successfully saved cleaned data for region %s at %s",
            self.region_filter,
            self.output_file_path,
        )

    def load_data(self, input_file_path: Path) -> pd.DataFrame:
        """
        This method loads a raw file with european life expectancy data over the years.
        """
        try:
            # load data
            with open(input_file_path, encoding="utf-8") as filename:
                df_raw = pd.read_csv(filename, sep="\t", na_values=[":"])
        except FileNotFoundError:
            logging.error("Input file %s not found.", input_file_path)
            return pd.DataFrame()
        logging.info("Successfully loaded %s", input_file_path)
        return df_raw
