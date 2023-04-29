"""Tests for the cleaning module"""
import pytest
import pandas as pd
from life_expectancy.region import Region

from life_expectancy.data_cleaning import (
    DataCleaner,
    JSONCleaningStrategy,
    CSVCleaningStrategy,
)


@pytest.mark.unit
def test_clean_data_with_csv_strategy(
    eu_life_expectancy_raw_expected, pt_life_expectancy_expected
):
    """Run the `clean_data` function with CSVCleaningStrategy
    and compare the output to the expected output"""
    datacleaner = DataCleaner(cleaning_strategy=CSVCleaningStrategy())
    pt_life_expectancy_actual = datacleaner.clean_data(
        eu_life_expectancy_raw_expected, region_filter=Region.PT
    )
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )


@pytest.mark.unit
def test_clean_data_with_json_strategy(
    eu_life_expectancy_raw_json, pt_life_expectancy_expected
):
    """Run the `clean_data` function with JSONCleaningStrategy
    and compare the output to the expected output"""
    datacleaner = DataCleaner(cleaning_strategy=JSONCleaningStrategy())
    pt_life_expectancy_actual = datacleaner.clean_data(
        eu_life_expectancy_raw_json, region_filter=Region.PT
    )
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
