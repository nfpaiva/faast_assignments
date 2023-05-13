"""Tests for the file_handler module"""

from unittest import mock
import json
from pathlib import Path
from typing import Union
import pytest
import pandas as pd
from life_expectancy.file_handler import (
    FileHandler,
    FileLoadingStrategy,
    CSVFileLoadingStrategy,
    ZipFileLoadingStrategy,
    JSONFileLoadingStrategy,
)
from life_expectancy.region import Region
from . import FIXTURES_DIR


@pytest.mark.unit
@mock.patch("life_expectancy.file_handler.CSVFileLoadingStrategy.load_data")
def test_load_data_with_csv_strategy(mock_load_data, eu_life_expectancy_raw_expected):
    """Run the `load_data` function with CSVFileLoadingStrategy
    and compare the output to the expected output"""
    filehandler = FileHandler(strategy=CSVFileLoadingStrategy())
    mock_load_data.return_value = eu_life_expectancy_raw_expected
    eu_life_expectancy_raw = filehandler.load_data(
        input_file_path=FIXTURES_DIR / "eu_life_expectancy_raw.tsv"
    )
    pd.testing.assert_frame_equal(
        eu_life_expectancy_raw_expected, eu_life_expectancy_raw
    )
    mock_load_data.assert_called_with(FIXTURES_DIR / "eu_life_expectancy_raw.tsv")


@pytest.mark.unit
@mock.patch("life_expectancy.file_handler.ZipFileLoadingStrategy.load_data")
def test_load_data_with_zip_strategy(mock_load_data, eu_life_expectancy_raw_json):
    """Run the `load_data` function with ZipFileLoadingStrategy
    and compare the output to the expected output"""
    filehandler = FileHandler(strategy=ZipFileLoadingStrategy())
    mock_load_data.return_value = eu_life_expectancy_raw_json
    eu_life_expectancy_raw = filehandler.load_data(
        input_file_path=FIXTURES_DIR / "eurostat_life_expect.zip"
    )
    pd.testing.assert_frame_equal(eu_life_expectancy_raw_json, eu_life_expectancy_raw)
    mock_load_data.assert_called_with(FIXTURES_DIR / "eurostat_life_expect.zip")


@pytest.mark.unit
@mock.patch("pandas.DataFrame.to_csv")
def test_save_data(mock_to_csv, pt_life_expectancy_expected):
    """Run the `save_data` function and compare the output to the expected output"""
    filehandler = FileHandler()
    output_file_path = "life_expectancy/data/pt_life_expectancy.csv"
    filehandler.save_data(pt_life_expectancy_expected, output_file_path, Region.PT)
    mock_to_csv.assert_called_with(output_file_path, index=False)


@pytest.mark.unit
def test_load_data_with_csv_strategy_file_not_found(caplog, non_existing_file_path_csv):
    """Run the `load_data` function with CSVFileLoadingStrategy
    using a non-existing file path and assert error handling"""
    filehandler = FileHandler(strategy=CSVFileLoadingStrategy())
    eu_life_expectancy_raw = filehandler.load_data(
        input_file_path=non_existing_file_path_csv
    )
    assert eu_life_expectancy_raw.empty
    assert "Input file non_existing_file_path.csv not found." in caplog.text


@pytest.mark.unit
def test_load_data_with_json_strategy_file_not_found(caplog, nonexistent_file_json):
    """Run the `load_data` function with JSONFileLoadingStrategy
    when file is not found"""
    filehandler = FileHandler(strategy=JSONFileLoadingStrategy())
    eu_life_expectancy_raw = filehandler.load_data(
        input_file_path=FIXTURES_DIR / nonexistent_file_json
    )
    assert eu_life_expectancy_raw.empty
    # Assert logging output
    assert "not found" in caplog.text


@pytest.mark.unit
def test_load_data_with_zip_strategy_file_not_found(caplog):
    """Test load_data with ZipFileLoadingStrategy
    when the input file is not found"""
    filehandler = FileHandler(strategy=ZipFileLoadingStrategy())
    input_file_path = "tests/fixtures/nonexistent_file.zip"
    eu_life_expectancy_raw = filehandler.load_data(input_file_path)
    assert eu_life_expectancy_raw.empty

    # Assert logging output
    assert "not found." in caplog.text


def test_load_data_with_zip_strategy_bad_zip_file(
    caplog, eu_life_expectancy_zip_file_bad_zip
):
    """Test load_data with ZipFileLoadingStrategy
    when the zip file is not valid"""
    filehandler = FileHandler(strategy=ZipFileLoadingStrategy())
    input_file_path = FIXTURES_DIR / eu_life_expectancy_zip_file_bad_zip
    eu_life_expectancy_raw = filehandler.load_data(input_file_path)
    assert eu_life_expectancy_raw.empty

    # Assert logging output
    assert "not a valid zip file" in caplog.text


def test_load_data_with_no_strategy(caplog):
    """Run the `load_data` function when no
    strategy is set and assert error handling"""
    filehandler = FileHandler()
    eu_life_expectancy_raw = filehandler.load_data(
        input_file_path="tests/fixtures/eu_life_expectancy_raw.tsv"
    )
    assert eu_life_expectancy_raw.empty
    # Assert logging output
    assert "No file handling strategy has been set." in caplog.text


@pytest.mark.unit
@mock.patch("pandas.DataFrame.to_csv")
def test_save_data_with_none_dataframe(mock_to_csv, caplog):
    """Test save_data method when dataframe is None"""
    filehandler = FileHandler()
    output_file_path = "life_expectancy/data/pt_life_expectancy.csv"
    df_final = None
    filehandler.save_data(df_final, output_file_path, Region.PT)
    assert not mock_to_csv.called
    assert "The final dataframe is None. Nothing will be saved." in caplog.text


@pytest.mark.unit
@mock.patch("pandas.DataFrame.to_csv", side_effect=PermissionError)
def test_save_data_with_permission_error(mock_to_csv, caplog):
    """Test save_data method when there is a permission error"""
    filehandler = FileHandler()
    output_file_path = "life_expectancy/data/pt_life_expectancy.csv"
    df_final = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
    filehandler.save_data(df_final, output_file_path, Region.PT)
    mock_to_csv.assert_called_once_with(output_file_path, index=False)
    assert "could not be created or written to." in caplog.text


@pytest.mark.unit
def test_load_data_not_implemented():
    """Test that the NotImplementedError is raised by the FileLoadingStrategy.load_data method"""

    class CustomLoadingStrategy(FileLoadingStrategy):
        """Class that defines a customloadingstrategy"""

        def load_data(self, input_file_path: Union[str, Path]) -> pd.DataFrame:
            # Custom implementation or raise NotImplementedError
            raise NotImplementedError

        def load_data_from_content(self, content: str) -> pd.DataFrame:
            # Custom implementation or raise NotImplementedError
            raise NotImplementedError

    strategy = CustomLoadingStrategy()
    with pytest.raises(NotImplementedError):
        strategy.load_data("/path/to/file")


@pytest.mark.unit
def test_load_data_from_content():
    """Test that the NotImplementedError is raised
    by the FileLoadingStrategy.load_data_from_content method"""
    # Prepare
    content = '{"col1": [1, 2, 3], "col2": [4, 5, 6]}'
    data = json.loads(content)
    expected = pd.DataFrame(data)

    # Test
    strategy = JSONFileLoadingStrategy()
    result = strategy.load_data_from_content(content)

    # Assert
    pd.testing.assert_frame_equal(expected, result)


@pytest.mark.unit
def test_zip_file_loading_strategy_load_data(eu_life_expectancy_raw_json):
    """Test that the ZipFileLoadingStrategy.load_data method works as expected"""
    # Load the data from the zip file
    file_handler = ZipFileLoadingStrategy()
    df = file_handler.load_data(
        input_file_path=FIXTURES_DIR / "eu_life_expectancy_expected_good.zip"
    )

    # Check that the data was loaded correctly

    pd.testing.assert_frame_equal(df, eu_life_expectancy_raw_json)
