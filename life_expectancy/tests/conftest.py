"""Pytest configuration file"""
import json
import pandas as pd
import pytest
from . import FIXTURES_DIR, OUTPUT_DIR


@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    """Fixture to execute commands before and after a test is run"""
    # Setup: fill with any logic you want

    yield  # this is where the testing happens

    # Teardown : fill with any logic you want
    file_path = OUTPUT_DIR / "pt_life_expectancy.csv"
    file_path.unlink(missing_ok=True)


@pytest.fixture(scope="session")
def pt_life_expectancy_expected() -> pd.DataFrame:
    """Fixture to load the expected output of the cleaning script"""
    return pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv")


@pytest.fixture(scope="session")
def eu_life_expectancy_raw_expected() -> pd.DataFrame:
    """Fixture to load the raw life expectancy data"""
    return pd.read_csv(
        FIXTURES_DIR / "eu_life_expectancy_raw.tsv", sep="\t", na_values=[":"]
    )


@pytest.fixture(scope="session")
def eu_life_expectancy_raw_json() -> pd.DataFrame:
    """Fixture to load the raw life expectancy data json format"""
    with open(FIXTURES_DIR / "eu_life_expectancy_expected.json", encoding="utf-8") as f:
        content = f.read()
    return pd.DataFrame(json.loads(content))


@pytest.fixture
def eu_life_expectancy_zip_file():
    """Fixture to load the raw life expectancy data zip format"""
    return "eu_life_expectancy.zip"


@pytest.fixture
def eu_life_expectancy_zip_file_bad_zip():
    """Fixture to load the raw life expectancy data zip format"""
    return "eu_life_expectancy_bad.zip"


@pytest.fixture
def nonexistent_file_json():
    """Fixture to load the raw life expectancy data zip format"""
    return "nonexistent_file.json"


@pytest.fixture
def non_existing_file_path_csv():
    """Fixture to load the raw life expectancy data zip format"""
    return "non_existing_file_path.csv"
