from extract import fetch_stock_data
from transform import transform_stock_data
from load import load_to_postgres

def run_pipeline(symbol="NVDA"):
    print("Extracting data...")
    raw_data = fetch_stock_data(symbol)

    print("Transforming data...")
    df = transform_stock_data(raw_data, symbol)

    print("Loading data into PostgreSQL...")
    load_to_postgres(df)

    print("Pipeline completed successfully.")

# Run the pipeline for a specific stock symbol (e.g., "NVDA")
if __name__ == "__main__":
    run_pipeline("NVDA")