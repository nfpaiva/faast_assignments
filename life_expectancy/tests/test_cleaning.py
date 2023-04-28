"""Tests for the cleaning module"""
import pytest
import pandas as pd


from life_expectancy.data_cleaning import DataCleaner


@pytest.mark.unit
def test_clean_data(eu_life_expectancy_raw_expected, pt_life_expectancy_expected):
    """Run the `clean_data` function and compare the output to the expected output"""
    datacleaner = DataCleaner()
    pt_life_expectancy_actual = datacleaner.clean_data(
        eu_life_expectancy_raw_expected, region_filter="PT"
    )
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
