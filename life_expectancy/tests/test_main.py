"""Tests for the main module."""
from unittest import mock
import pandas as pd
from life_expectancy.main import loading_cleaning_saving


@mock.patch("life_expectancy.main.FileHandler.load_data")
@mock.patch("life_expectancy.main.DataCleaner.clean_data")
@mock.patch("life_expectancy.main.FileHandler.save_data")
def test_loading_cleaning_saving(
    mock_save_data,
    mock_clean_data,
    mock_load_data,
    pt_life_expectancy_expected,
    eu_life_expectancy_raw_expected,
):
    """Test loading_cleaning_saving function."""
    mock_load_data.return_value = (
        eu_life_expectancy_raw_expected  # Specify return value
    )
    mock_clean_data.return_value = pt_life_expectancy_expected

    result = loading_cleaning_saving("PT")

    mock_load_data.assert_called_once_with(mock.ANY)
    mock_clean_data.assert_called_once_with(eu_life_expectancy_raw_expected, "PT")
    mock_save_data.assert_called_once_with(pt_life_expectancy_expected, mock.ANY, "PT")

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, pt_life_expectancy_expected)
