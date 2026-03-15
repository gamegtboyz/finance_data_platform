import pytest
import pandas as pd
from datetime import date

from src.loaders.fact_loader import load_stock_prices, get_max_loaded_date
from src.loaders.dimension_loader import load_dim_dates, load_dim_metadata

# define the helper functions for further testing as show below
def _dim_date_row(date_str):
    dt = pd.Timestamp(date_str)
    return {
        "date": dt,
        "day": dt.day,
        "month": dt.month,
        "year": dt.year,
        "quarter": ((dt.month - 1) // 3) + 1,
        "day_of_week": dt.dayofweek,
        "week_of_year": int(dt.isocalendar().week)
    }

def _price_row(symbol, date_str, opening=148.0, closing=150.0):
    dt = pd.Timestamp(date_str)
    return {
        "symbol": symbol,
        "date": dt,
        "open": opening,
        "high": closing + 1,
        "low":  opening - 1,
        "close": closing,
        "volume": 25000000,
        "day": dt.day,
        "month": dt.month,
        "year": dt.year,
        "quarter": ((dt.month - 1) // 3) + 1,
        "day_of_week": dt.dayofweek,
        "week_of_year": int(dt.isocalendar().week)
    }

# create the class of testing methods
@pytest.mark.integration
class TestLoadDimDates:
    def test_inserts_rows(self, db_cursor):
        """
        Given the date dimension table is empty,
        when we load a date, then it should be inserted into the table in just one row.
        """
        df = pd.DataFrame([_dim_date_row("2026-03-09")])
        load_dim_dates(db_cursor, df)
        db_cursor.execute("SELECT COUNT(*) FROM dim_date;")
        assert db_cursor.fetchone()[0] == 1

    def test_idempotent_on_conflict(self, db_cursor):
        """
        When we load the dates twice, only one row of data should be inserted into the database
        """
        df = pd.DataFrame([_dim_date_row("2026-03-09")])
        load_dim_dates(db_cursor, df)
        load_dim_dates(db_cursor, df)
        db_cursor.execute("SELECT COUNT(*) FROM dim_date;")
        assert db_cursor.fetchone()[0] == 1

    def test_deduplicate_within_dataframe(self, db_cursor):
        """
        Given the date dimension table is empty,
        when we load a dataframe with duplicate dates, only unique dates should be inserted into the table.
        """
        df = pd.DataFrame([_dim_date_row("2026-03-09"), _dim_date_row("2026-03-09")])
        load_dim_dates(db_cursor, df)
        db_cursor.execute("SELECT COUNT(*) FROM dim_date;")
        assert db_cursor.fetchone()[0] == 1

    def test_inserts_multiple_distinct_dates(self, db_cursor):
        """
        Given put multiple distince dates, those 2 different dates should be loaded into the database
        """
        df = pd.DataFrame([_dim_date_row("2026-03-09"), _dim_date_row("2026-03-08")])
        load_dim_dates(db_cursor, df)
        db_cursor.execute("SELECT COUNT(*) FROM dim_date;")
        assert db_cursor.fetchone()[0] == 2

@pytest.mark.integration
class TestLoadDimMetadata:
    # define the metadata variable as a pytest fixture to be used in the testing methods below
    @pytest.fixture
    def metadata(self):
        return {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "sector": "TECHNOLOGY"
        }
    
    def test_inserts_metadata(self, db_cursor, metadata):
        """
        Given the metadata dimension table is empty,
        when we load a metadata record, then it should be inserted into the table in just one row.
        """
        load_dim_metadata(db_cursor, metadata)
        db_cursor.execute("SELECT * FROM dim_metadata")
        assert db_cursor.fetchone() == ("AAPL", "Apple Inc.", "TECHNOLOGY")

    def test_idempotent_on_conflict(self, db_cursor, metadata):
        """
        Given the blank dim_metadata table,
        ensure that when we load the identical data twice, the database will keep only one.
        """
        load_dim_metadata(db_cursor, metadata)
        load_dim_metadata(db_cursor, metadata)
        db_cursor.execute("SELECT COUNT(*) FROM dim_metadata;")
        assert db_cursor.fetchone()[0] == 1

@pytest.mark.integration
class TestLoadStockPrices:
    def _seed(self, db_cursor, dates=("2026-03-09", "2026-03-08"), symbol="AAPL"):
        """
        Helper method to seed the dim_date and dim_metadata tables with the necessary data for testing the fact loader.
        """
        dim_df = pd.DataFrame([_dim_date_row(d) for d in dates])
        load_dim_dates(db_cursor, dim_df)
        load_dim_metadata(db_cursor, {"symbol": symbol, "company_name": "Apple Inc.", "sector": "TECHNOLOGY"})

    def test_inserts_rows(self, db_cursor):
        """
        Given the blank stock_prices fact table,
        when we load a stock price record,
        then it should be inserted into the table in just one row.
        """
        self._seed(db_cursor)
        df = pd.DataFrame([_price_row("AAPL", "2026-03-09"), _price_row("AAPL", "2026-03-08")])
        load_stock_prices(db_cursor, df)
        db_cursor.execute("SELECT COUNT(*) FROM stock_prices;")
        assert db_cursor.fetchone()[0] == 2

    def test_idempotent_on_conflict(self, db_cursor):
        """
        Given blank stock_prices table,
        when two identical dataframes are loaded,
        only one row of data should be inserted into the database.
        """
        self._seed(db_cursor, dates=("2026-03-09",))
        load_stock_prices(db_cursor, pd.DataFrame([_price_row("AAPL", "2026-03-09")]))
        load_stock_prices(db_cursor, pd.DataFrame([_price_row("AAPL", "2026-03-09")]))
        db_cursor.execute("SELECT COUNT(*) FROM stock_prices;")
        assert db_cursor.fetchone()[0] == 1

    def test_currect_values_are_sorted(self, db_cursor):
        """
        Given the blank stock_prices table,
        the OHLCV stored should exactly match what was loaded
        """
        self._seed(db_cursor, dates=("2026-03-09",))
        load_stock_prices(db_cursor, pd.DataFrame([_price_row("AAPL", "2026-03-09", opening=148.0, closing=150.0)]))
        db_cursor.execute("SELECT open, high, low, close, volume FROM stock_prices WHERE symbol = 'AAPL';")
        assert db_cursor.fetchone() == (148.0, 151.0, 147.0, 150.0, 25000000)

    def test_get_max_loaded_date(self, db_cursor):
        """
        get max_loaded_date should return the latest date present in stock_prices for the given symbol.
        """
        self._seed(db_cursor, dates=("2026-03-09", "2026-03-08"))
        df = pd.DataFrame([_price_row("AAPL", "2026-03-09"),_price_row("AAPL", "2026-03-08")])
        load_stock_prices(db_cursor, df)
        result = get_max_loaded_date(db_cursor, "AAPL")
        assert result == date(2026, 3, 9)