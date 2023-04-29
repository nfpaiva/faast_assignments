"""Tests for the main module."""
from unittest import mock
from pathlib import Path
import pandas as pd
from life_expectancy.main import loading_cleaning_saving
from life_expectancy.region import Region


@mock.patch("life_expectancy.main.FileHandler.load_data")
@mock.patch("life_expectancy.main.DataCleaner.clean_data")
@mock.patch("life_expectancy.main.FileHandler.save_data")
def test_loading_cleaning_saving_csv(
    mock_save_data,
    mock_clean_data,
    mock_load_data,
    pt_life_expectancy_expected,
    eu_life_expectancy_raw_expected,
):
    """Test loading_cleaning_saving function for CSV file type."""
    mock_load_data.return_value = eu_life_expectancy_raw_expected
    mock_clean_data.return_value = pt_life_expectancy_expected

    result = loading_cleaning_saving(Region.PT, Path("test.csv"), ".csv")

    mock_load_data.assert_called_once_with(Path("test.csv"))
    mock_clean_data.assert_called_once_with(eu_life_expectancy_raw_expected, Region.PT)
    mock_save_data.assert_called_once_with(
        pt_life_expectancy_expected, mock.ANY, Region.PT
    )

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, pt_life_expectancy_expected)


@mock.patch("life_expectancy.main.FileHandler.load_data")
@mock.patch("life_expectancy.main.DataCleaner.clean_data")
@mock.patch("life_expectancy.main.FileHandler.save_data")
def test_loading_cleaning_saving_json(
    mock_save_data,
    mock_clean_data,
    mock_load_data,
    pt_life_expectancy_expected,
    eu_life_expectancy_raw_json,
):
    """Test loading_cleaning_saving function for JSON file type."""
    mock_load_data.return_value = eu_life_expectancy_raw_json
    mock_clean_data.return_value = pt_life_expectancy_expected

    result = loading_cleaning_saving(Region.PT, Path("test.json"), ".json")

    mock_load_data.assert_called_once_with(Path("test.json"))
    mock_clean_data.assert_called_once_with(eu_life_expectancy_raw_json, Region.PT)
    mock_save_data.assert_called_once_with(
        pt_life_expectancy_expected, mock.ANY, Region.PT
    )

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, pt_life_expectancy_expected)
