"""Tests for the main function."""
import pandas as pd
import pytest
from life_expectancy.main import main


@pytest.mark.integration
def test_main(pt_life_expectancy_expected):
    """Test with a country that exists in the data"""
    country = "PT"
    pt_life_expectancy_actual = main(country)
    pd.testing.assert_frame_equal(
        pt_life_expectancy_actual, pt_life_expectancy_expected
    )
