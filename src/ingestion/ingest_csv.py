import sys
import pandas as pd
from pathlib import Path

# Locate project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# CSV source file
CSV_FILE = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "csv"
    / "store_orders.csv"
)

# Required columns
REQUIRED_COLUMNS = [
    "order_id",
    "order_line_id",
    "customer_id",
    "product_id",
    "product_name",
    "quantity",
    "unit_price",
    "discount_amount",
    "order_date",
    "channel",
    "store_id",
    "updated_at"
]



def ingest_csv():

    print("Starting CSV ingestion...")

    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"CSV file not found: {CSV_FILE}"
        )

    try:
        df = pd.read_csv(CSV_FILE)

        print("\nCSV file loaded successfully!")

        missing_columns = [
            col for col in REQUIRED_COLUMNS
            if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        print("Schema validation: PASSED")
        print("Total records received:", len(df))

        duplicate_count = df.duplicated(
            subset=["order_line_id"]
        ).sum()

        print("Duplicate records found:", duplicate_count)

        df["order_date"] = pd.to_datetime(
            df["order_date"],
            errors="raise"
        )

        print("\nSample CSV records:")
        print(df.head())

        print("\nCSV ingestion completed successfully!")

    except Exception as error:
        print("CSV ingestion failed:", error)
        raise

if __name__ == "__main__":
    ingest_csv()