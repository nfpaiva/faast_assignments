"""Module to represent different regions """
import enum
from typing import List
import pandas as pd


class Region(enum.Enum):
    """Enum to represent different regions"""

    PT = "PT"
    ES = "ES"
    FR = "FR"
    DE = "DE"
    IT = "IT"
    BE = "BE"
    NL = "NL"
    LU = "LU"
    UK = "UK"
    IE = "IE"
    DK = "DK"
    SE = "SE"
    FI = "FI"
    EE = "EE"
    LV = "LV"
    LT = "LT"
    PL = "PL"
    CZ = "CZ"
    SK = "SK"
    HU = "HU"
    SI = "SI"
    AT = "AT"
    CH = "CH"

    @classmethod
    def get_actual_countries(cls, df: pd.DataFrame, col_name: str) -> List[str]:
        """Returns a list of all the actual countries present in the input data
        that match the enum values"""
        countries = set(df[col_name].unique())
        actual_countries = [c.value for c in cls if c.value in countries]
        return actual_countries
