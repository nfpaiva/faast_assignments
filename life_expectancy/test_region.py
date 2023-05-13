"""Tests for the Region class."""

import pandas as pd
from life_expectancy.region import Region


def test_get_actual_countries():
    """Test the get_actual_countries method."""
    # create a sample dataframe with regions that match the Region enum values
    df = pd.DataFrame({"region": ["PT", "ES", "FR", "DE"]})

    # get the list of actual countries
    actual_countries = Region.get_actual_countries(df, "region")

    # assert that the list of actual countries matches the expected list
    expected_countries = ["PT", "ES", "FR", "DE"]
    assert actual_countries == expected_countries

    # create a sample dataframe with regions that don't match the Region enum values
    df = pd.DataFrame({"region": ["US", "CN", "JP"]})

    # get the list of actual countries
    actual_countries = Region.get_actual_countries(df, "region")

    # assert that the list of actual countries matches the expected list
    expected_countries = []
    assert actual_countries == expected_countries
