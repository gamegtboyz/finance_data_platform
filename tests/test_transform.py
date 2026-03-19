import pytest
import pandas as pd
from datetime import datetime
import json
import tempfile
import os
from src.transform.transform_stock import transform_stock_prices, transform_company_metadata


# we focused on testing on transformation phase. So, each class represents testing process on transformation method.
class TestTransformStockPrices:
    
    @pytest.fixture
    def sample_api_response(self):
        """
        Create a mock API response JSON to interact with subsequent tests.
        This simulates the structure of the AlphaVantage daily time series response.
        """
        return {
            "Time Series (Daily)": {
                "2026-03-09": {
                    "1. open": "150.00",
                    "2. high": "152.30",
                    "3. low": "149.50",
                    "4. close": "151.25",
                    "5. volume": "25000000"
                },
                "2026-03-08": {
                    "1. open": "149.00",
                    "2. high": "151.00",
                    "3. low": "148.50",
                    "4. close": "150.50",
                    "5. volume": "24000000"
                }
            }
        }

    def test_transform_returns_dataframe(self, sample_api_response):
        """
        Ensure that transformed data is a pandas DataFrame.
        """
        # create a temporary JSON file with the sample API response
        with tempfile.NamedTemporaryFile(mode="w", suffix='.json', delete=False) as f:
            json.dump(sample_api_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "AAPL")
            assert isinstance(df, pd.DataFrame)
        finally:
            os.unlink(temp_filepath)

    def test_transform_has_required_columns(self, sample_api_response):
        """
        Ensure that the transformed output has all required columns.
        """
        # create a temporary JSON file with the sample API response
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_api_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "AAPL")
            required_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "day", "month", "year", "quarter"]
            assert all(col in df.columns for col in required_cols)
        finally:
            os.unlink(temp_filepath)

    def test_transform_correct_data_types(self, sample_api_response):
        """
        Ensure that the transformed DataFrame has correct data types.
        """
        # create a temporary JSON file with the sample API response
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_api_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "AAPL")
            assert df["symbol"].dtype in ["object", "string"]
            assert df["open"].dtype in ["float64", "float32"]
            assert df["high"].dtype in ["float64", "float32"]
            assert df["low"].dtype in ["float64", "float32"]
            assert df["close"].dtype in ["float64", "float32"]
            assert df["volume"].dtype in ["int64", "int32"]
        finally:
            os.unlink(temp_filepath)

    def test_transform_symbol_parameter(self, sample_api_response):
        """
        Ensure that symbol is properly included.
        """
        # create a temporary JSON file with the sample API response
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_api_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "NVDA")
            assert (df["symbol"] == "NVDA").all()
        finally:
            os.unlink(temp_filepath)

class TestTransformCompanyMetadata:

    @pytest.fixture
    def sample_metadata_response(self):
        """
        Create a mock metadata API response to interact with the following testing methods.
        """
        return {
            "Symbol": "AAPL",
            "Name": "Apple Inc.",
            "Sector": "Technology",
            "Industry": "Consumer Electronics"
        }
    
    def test_transform_metadata_returns_dict(self, sample_metadata_response):
        """
        Ensure that transformed data is a dictionary.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_metadata_response, f)
            temp_filepath = f.name

        try:
            result = transform_company_metadata(temp_filepath)
            assert isinstance(result, dict)
        finally:
            os.unlink(temp_filepath)

    def test_transform_metadata_has_required_keys(self, sample_metadata_response):
        """
        Ensure that transformed output has all required keys.
        """
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(sample_metadata_response, f)
            temp_filepath = f.name

        try:
            result = transform_company_metadata(temp_filepath)
            assert "symbol" in result
            assert "company_name" in result
            assert "sector" in result
        finally:
            os.unlink(temp_filepath)

    def test_transform_metadata_missing_symbol_raises(self):
        """
        Ensure that missing Symbol field raises ValueError.
        """

        bad_response = {"Name": "Apple Inc."}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(bad_response, f)
            temp_filepath = f.name

        try:
            with pytest.raises(ValueError, match="missing 'Symbol'"):
                transform_company_metadata(temp_filepath)
        finally:
            os.unlink(temp_filepath)

    def test_transform_metadata_quota_response_raises(self):
        """
        Ensure that a quota-exceeded API response stored in a metadata file raises ValueError.
        This guards against silently loading garbage data when AlphaVantage returns a
        rate-limit or quota message instead of real metadata.
        """
        quota_response = {"Information": "Thank you for using Alpha Vantage! Our premium..."}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(quota_response, f)
            temp_filepath = f.name

        try:
            with pytest.raises(ValueError, match="API quota/rate limit"):
                transform_company_metadata(temp_filepath)
        finally:
            os.unlink(temp_filepath)

    def test_transform_metadata_rate_limit_response_raises(self):
        """
        Ensure that a per-minute rate-limit API response stored in a metadata file raises ValueError.
        """
        rate_limit_response = {"Note": "Thank you for using Alpha Vantage! Our standard API call frequency..."}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(rate_limit_response, f)
            temp_filepath = f.name

        try:
            with pytest.raises(ValueError, match="API quota/rate limit"):
                transform_company_metadata(temp_filepath)
        finally:
            os.unlink(temp_filepath)

class TestTransformStockPricesEdgeCases:

    def test_empty_time_series_returns_empty_dataframe(self):
        """
        Ensure that an API response with an empty Time Series returns an empty DataFrame
        rather than raising an error or silently loading incorrect data.
        """
        empty_response = {"Time Series (Daily)": {}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(empty_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "AAPL")
            assert isinstance(df, pd.DataFrame)
            assert df.empty
        finally:
            os.unlink(temp_filepath)

    def test_missing_time_series_key_returns_empty_dataframe(self):
        """
        Ensure that an API response missing the 'Time Series (Daily)' key (e.g. a quota
        or error response) returns an empty DataFrame rather than raising a KeyError.
        """
        bad_response = {"Information": "API limit reached"}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(bad_response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "AAPL")
            assert isinstance(df, pd.DataFrame)
            assert df.empty
        finally:
            os.unlink(temp_filepath)

    def test_malformed_price_value_raises(self):
        """
        Ensure that a non-numeric price value raises a ValueError during transformation.
        """
        bad_response = {
            "Time Series (Daily)": {
                "2026-03-09": {
                    "1. open": "N/A",
                    "2. high": "152.30",
                    "3. low": "149.50",
                    "4. close": "151.25",
                    "5. volume": "25000000"
                }
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(bad_response, f)
            temp_filepath = f.name

        try:
            with pytest.raises(ValueError):
                transform_stock_prices(temp_filepath, "AAPL")
        finally:
            os.unlink(temp_filepath)

    def test_quarter_calculation_correctness(self):
        """
        Verify the quarter derivation is correct for representative months across all four quarters.
        """
        response = {
            "Time Series (Daily)": {
                "2026-01-15": {"1. open": "100", "2. high": "101", "3. low": "99", "4. close": "100", "5. volume": "1000000"},
                "2026-04-15": {"1. open": "100", "2. high": "101", "3. low": "99", "4. close": "100", "5. volume": "1000000"},
                "2026-07-15": {"1. open": "100", "2. high": "101", "3. low": "99", "4. close": "100", "5. volume": "1000000"},
                "2026-10-15": {"1. open": "100", "2. high": "101", "3. low": "99", "4. close": "100", "5. volume": "1000000"},
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(response, f)
            temp_filepath = f.name

        try:
            df = transform_stock_prices(temp_filepath, "TEST")
            df = df.sort_values("date").reset_index(drop=True)
            assert df.loc[df["date"].dt.month == 1, "quarter"].iloc[0] == 1
            assert df.loc[df["date"].dt.month == 4, "quarter"].iloc[0] == 2
            assert df.loc[df["date"].dt.month == 7, "quarter"].iloc[0] == 3
            assert df.loc[df["date"].dt.month == 10, "quarter"].iloc[0] == 4
        finally:
            os.unlink(temp_filepath)