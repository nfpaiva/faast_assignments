""" This module contains the DataCleaner class, which is responsible for cleaning the raw data"""

from typing import List
import logging
from abc import ABC, abstractmethod
import pandas as pd
from life_expectancy.region import Region


class CleaningStrategy(ABC):
    """Abstract class for cleaning strategies"""

    @abstractmethod
    def clean(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Abstract method for cleaning data"""


class JSONCleaningStrategy(CleaningStrategy):
    """Concrete class for cleaning JSON data"""

    def clean(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        # Implement JSON cleaning strategy here
        df_final = pd.DataFrame(df_raw)
        df_final = df_final[
            ["unit", "sex", "age", "country", "year", "life_expectancy"]
        ]
        df_final = df_final.rename(
            columns={"country": "region", "life_expectancy": "value"}
        )

        return df_final


class CSVCleaningStrategy(CleaningStrategy):
    """Concrete class for cleaning CSV data"""

    def __init__(self, composed_col: str, decomposed_cols: List[str]):
        self.composed_col = composed_col
        self.decomposed_cols = decomposed_cols

    def clean(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df_raw = df_raw.copy()
        # Split first column into 4 columns
        df_raw[self.decomposed_cols] = df_raw[self.composed_col].str.split(
            ",", expand=True
        )
        df_raw = df_raw.drop(columns=[self.composed_col])

        # Transform data into long format, filter out missings
        df_final = pd.melt(df_raw, id_vars=self.decomposed_cols, var_name="year")

        # Convert column data types explicitly
        data_types = {
            "unit": "str",
            "sex": "str",
            "age": "str",
            "region": "str",
            "year": "int",
            "value": "float64",
        }

        column_groups = {
            value: [key for key, val in data_types.items() if val == value]
            for value in set(data_types.values())
        }

        def convert_datatypes(
            dataframe: pd.DataFrame, cols_2_convert: List[str], dtype: str
        ) -> pd.DataFrame:
            """
            This function iterates on each group of columns to convert to the determined data type
            """
            for col in cols_2_convert:
                if cols_2_convert == ["value"]:
                    try:
                        dataframe[col] = pd.to_numeric(
                            dataframe[col].str.extract(
                                r"(\d+(?:\.\d+)?)", expand=False
                            ),
                            errors="raise",
                        )
                    except ValueError as error:
                        logging.error("Datatype conversion error %s", error)
                else:
                    try:
                        dataframe[col] = dataframe[col].astype(
                            dtype=dtype, errors="raise"
                        )  # type: ignore
                    except ValueError as error:
                        logging.error("Datatype conversion error %s", error)
            return dataframe

        for dtype, cols in column_groups.items():
            df_final[cols] = convert_datatypes(df_final.loc[:, cols], cols, dtype)
        return df_final


class DataCleaner:
    """
    DataCleaner: Class responsible for cleaning European life expectancy data files.
    """

    class NoDataException(Exception):
        """
        NoDataException: Exception raised when no data is found for a given region
        """

    def __init__(self, cleaning_strategy: CleaningStrategy):
        """Constructor for DataCleaner"""
        self.cleaning_strategy = cleaning_strategy

    def clean_data(self, df_raw: pd.DataFrame, region_filter: Region) -> pd.DataFrame:
        """This method cleans the raw data and filters by region"""
        df_cleaned = self.cleaning_strategy.clean(df_raw)

        # Get the list of countries in the cleaned data
        countries = Region.get_actual_countries(df_cleaned, "region")

        # Validate the region filter
        if region_filter.value not in countries:
            raise ValueError(
                f"Invalid region: {region_filter}. Available regions: {countries}"
            )

        return self.filter_by_region(df_cleaned, region_filter)

    def filter_by_region(self, df: pd.DataFrame, region_filter: Region) -> pd.DataFrame:
        """This method filters the data by region and drops rows with missing values"""
        df_filtered = df[df["region"] == region_filter.value]

        if df_filtered.empty:
            raise DataCleaner.NoDataException(f"No data for region {region_filter}")

        df_filtered = df_filtered.dropna(subset=["value"])
        return df_filtered.reset_index(drop=True)
