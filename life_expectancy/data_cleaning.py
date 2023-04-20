"""
This module provides a function to clean european life expectancy data files.
"""
from typing import List
import logging
import pandas as pd


# Define variables cleaning  and filtering data
COMPOSED_COL = "unit,sex,age,geo\\time"
DECOMPOSED_COLs = ["unit", "sex", "age", "region"]


def clean_data(df_raw: pd.DataFrame, region_filter: str) -> pd.DataFrame:
    """
    This function processes and filters a dataframe
    with european life expectancy data over the years.

    Parameters:
    None

    Returns:
    Just saves the processed data for Portugal and returns also pd dataframe for test fixtures
    """
    df_raw = df_raw.copy()
    # Split first column into 4 columns
    df_raw[DECOMPOSED_COLs] = df_raw[COMPOSED_COL].str.split(",", expand=True)
    df_raw = df_raw.drop(columns=[COMPOSED_COL])

    # Transform data into long format, filter out missings and region
    df_final = pd.melt(df_raw, id_vars=DECOMPOSED_COLs, var_name="year")

    # convert column data types explicitly
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
                        dataframe[col].str.extract(r"(\d+(?:\.\d+)?)", expand=False),
                        errors="raise",
                    )
                except ValueError as error:
                    logging.error("Datatype conversion error %s", error)
            else:
                try:
                    dataframe[col] = dataframe[col].astype(dtype=dtype, errors="raise")
                except ValueError as error:
                    logging.error("Datatype conversion error %s", error)
        return dataframe

    for dtype, cols in column_groups.items():
        df_final[cols] = convert_datatypes(df_final.loc[:, cols], cols, dtype)

    df_final = df_final[df_final["region"] == region_filter]
    df_final = df_final.dropna(subset=["value"])

    df_final = df_final[(df_final["region"] == region_filter)]

    if df_final.empty:  # pragma: no cover
        logging.error("No data found for region %s", region_filter)
    else:
        logging.info("Successfully cleaned data for region== %s", region_filter)
        return df_final.reset_index(drop=True)
