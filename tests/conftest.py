import pytest
import psycopg2
from src.db_connect import db_connect

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "integration: mark a test as an integration test that requiring a live PostgreSQL connection (docker compose up -d)"
    )

@pytest.fixture(scope="function")
def db_cursor():
    """
    Connect to the real Postgres instance and create TEMP tables
    , which mimics the production schema. Each test gets a clean slate;
    connection is rolled back and closed after the test regardless of pass/fail.

    Skip automatically if Postgres is not reachable.
    """
    try:
        conn = db_connect()
    except psycopg2.OperationalError:
        pytest.skip("PostgreSQL is not reachable, compose the docker container first via: docker compose up -d")

    conn.autocommit = False
    cursor = conn.cursor()

    """
    TEMP tables are session-scoped and don't support cross-table foreign keys (FK) constraints,
    so they are defined without them here. They are auto-dropped when the connection is closed .
    """
    # create temp date dimension table
    cursor.execute(
        """
        CREATE TEMP TABLE dim_date (
            date            DATE PRIMARY KEY,
            day             INT,
            month           INT,
            year            INT,
            quarter         INT,
            day_of_week     INT,
            week_of_year    INT
        );
        """
    )

    # create temp metadata dimension table
    cursor.execute(
        """
        CREATE TEMP TABLE dim_metadata (
            symbol         TEXT PRIMARY KEY,
            company_name   TEXT,
            sector         TEXT
        );
        """
    )

    # create temp fact table
    cursor.execute(
        """
        CREATE TEMP TABLE stock_prices (
            symbol         TEXT,
            date           DATE,
            open           NUMERIC,
            high           NUMERIC,
            low            NUMERIC,
            close          NUMERIC,
            volume         BIGINT,
            PRIMARY KEY (symbol, date)
        );
        """
    )

    yield cursor    # yield the cursor to the test function. This was used to separate the setup and teardown logic, which is a best practice for test fixtures.
    conn.rollback() # rollback any changes to ensure test isolation
    cursor.close()
    conn.close()