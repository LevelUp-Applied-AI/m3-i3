"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest


def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # TODO: Implement
    pass


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # TODO: Implement
    pass


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # TODO: Implement
    pass
