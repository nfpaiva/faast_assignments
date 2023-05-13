"""
This module provides a class to load and save files.
"""

from pathlib import Path
import logging
from io import StringIO
import json
import zipfile
from typing import Union
from abc import ABC, abstractmethod
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)


class FileHandler(ABC):
    """Class to load and save files."""

    def __init__(self, strategy=None):
        self.strategy = strategy

    def save_data(
        self, df_final: pd.DataFrame, output_file_path: Path, region_filter: str
    ) -> None:
        """Save the final dataframe to a file."""
        if df_final is None:
            logging.warning("The final dataframe is None. Nothing will be saved.")
            return

        try:
            df_final.to_csv(output_file_path, index=False)
        except PermissionError:
            logging.error(
                "Output file %s could not be created or written to.", output_file_path
            )
            return

        logging.info(
            "Successfully saved cleaned data for region %s at %s",
            region_filter,
            output_file_path,
        )

    @abstractmethod
    def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
        """Load the data from a file."""
        if self.strategy:
            return self.strategy.load_data(input_file_path)
        logging.error("No file handling strategy has been set.")
        return pd.DataFrame()


class FileLoadingStrategy(ABC):
    """Interface for loading files."""

    @abstractmethod
    def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
        """Load the data from a file."""
        raise NotImplementedError

    @abstractmethod
    def load_data_from_content(self, content: str) -> pd.DataFrame:
        """Load the data from a string."""
        raise NotImplementedError


class CSVFileLoadingStrategy(FileLoadingStrategy):
    """Class to load CSV files."""

    def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
        """Load the data from a file."""
        try:
            with open(Path(input_file_path), encoding="utf-8") as filename:
                df_raw = pd.read_csv(filename, sep="\t", na_values=[":"])
        except FileNotFoundError:
            logging.error("Input file %s not found.", input_file_path)
            return pd.DataFrame()
        logging.info("Successfully loaded %s", input_file_path)
        return df_raw

    def load_data_from_content(self, content: str) -> pd.DataFrame:
        """Load the data from a string."""
        return pd.read_csv(StringIO(content), sep="\t", na_values=[":"])


class JSONFileLoadingStrategy(FileLoadingStrategy):
    """Class to load JSON files."""

    def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
        """Load the data from a file."""
        try:
            with open(Path(input_file_path), encoding="utf-8") as filename:
                data = json.load(filename)
                df_raw = pd.DataFrame(data)
        except FileNotFoundError:
            logging.error("Input file %s not found.", input_file_path)
            return pd.DataFrame()
        logging.info("Successfully loaded %s", input_file_path)
        return df_raw

    def load_data_from_content(self, content: str) -> pd.DataFrame:
        """Load the data from a string."""
        data = json.loads(content)
        return pd.DataFrame(data)


class ZipFileLoadingStrategy(FileLoadingStrategy):
    """Class to load zip files."""

    def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
        if not Path(input_file_path).exists():
            logging.error("Input file %s not found.", input_file_path)
            return pd.DataFrame()

        strategies = {
            ".csv": CSVFileLoadingStrategy,
            ".tsv": CSVFileLoadingStrategy,
            ".json": JSONFileLoadingStrategy,
        }

        try:
            with zipfile.ZipFile(Path(input_file_path), "r") as zip_ref:
                file = zip_ref.namelist()[0]
                file_ext = Path(file).suffix.lower()
                try:
                    strategy = strategies[file_ext]()
                except KeyError as err:
                    logging.error("Unsupported file type inside zip: %s", file_ext)
                    raise err

                with zip_ref.open(file, "r") as file_ref:
                    file_content = file_ref.read().decode("utf-8")
                    return strategy.load_data_from_content(file_content)
        except zipfile.BadZipFile:
            logging.error("Input file %s is not a valid zip file.", input_file_path)
            return pd.DataFrame()

    def load_data_from_content(self, content: str) -> pd.DataFrame:
        """Load the data from a string."""
        file_content = content
        return self.load_data(file_content)
