"""Tests for the main module."""
from unittest import mock
from life_expectancy.main import loading_cleaning_saving


@mock.patch("life_expectancy.main.load_data")
@mock.patch("life_expectancy.main.clean_data")
@mock.patch("life_expectancy.main.save_data")
def test_loading_cleaning_saving(
    mock_save_data,
    mock_clean_data,
    mock_load_data,
    pt_life_expectancy_expected,
    eu_life_expectancy_raw_expected,
):
    """Test loading_cleaning_saving function."""
    mock_load_data.return_value = eu_life_expectancy_raw_expected
    mock_clean_data.return_value = pt_life_expectancy_expected

    result = loading_cleaning_saving("PT")

    mock_load_data.assert_called_once()
    mock_clean_data.assert_called_once()
    mock_save_data.assert_called_once()

    assert result.equals(pt_life_expectancy_expected)
