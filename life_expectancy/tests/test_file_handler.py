"""Tests for the file_handler module"""

from unittest import mock
import pandas as pd
import pytest
from life_expectancy.file_handler import load_data, save_data


@pytest.mark.unit
def test_load_date(eu_life_expectancy_raw_expected):
    """Run the `load_data` function and compare the output to the expected output"""
    eu_life_expectancy_raw = load_data(
        input_file_path="life_expectancy/data/eu_life_expectancy_raw.tsv"
    )
    pd.testing.assert_frame_equal(
        eu_life_expectancy_raw_expected, eu_life_expectancy_raw
    )


@pytest.mark.unit
@mock.patch("pandas.DataFrame.to_csv")
def test_save_data(mock_to_csv, pt_life_expectancy_expected):
    """Run the `save_data` function and compare the output to the expected output"""
    output_file_path = "life_expectancy/data/pt_life_expectancy.csv"
    save_data(pt_life_expectancy_expected, output_file_path, "PT")
    mock_to_csv.assert_called_with(output_file_path, index=False)
