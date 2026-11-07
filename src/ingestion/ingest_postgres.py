import os
import pandas as pd

from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, URL, text

# Find the project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load database credentials from .env
load_dotenv(PROJECT_ROOT / ".env")

# Read database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Output folder
OUTPUT_FOLDER = PROJECT_ROOT / "data" / "raw" / "postgres"
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)



def ingest_postgres():

    print("Starting PostgreSQL ingestion...")

    engine = None

    try:
        # Check that all database settings are available
        required_settings = {
            "DB_HOST": DB_HOST,
            "DB_PORT": DB_PORT,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASSWORD": DB_PASSWORD
        }

        missing_settings = [
            name for name, value in required_settings.items()
            if not value
        ]

        if missing_settings:
            raise ValueError(
                f"Missing database settings: {missing_settings}"
            )

        # Create database connection
        connection_url = URL.create(
            drivername="postgresql+psycopg2",
            username=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME
        )

        engine = create_engine(connection_url)

        # Check database connection
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

        print("PostgreSQL connection successful!")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Extract customers
        customers_df = pd.read_sql_query(
            "SELECT * FROM retail_customers ORDER BY customer_id",
            engine
        )

        if customers_df.empty:
            raise ValueError("Customer table is empty")

        customers_file = (
            OUTPUT_FOLDER / f"retail_customers_{timestamp}.csv"
        )

        customers_df.to_csv(customers_file, index=False)

        print("\nCustomer extraction successful!")
        print("Customer records:", len(customers_df))
        print("Saved:", customers_file)

        # Extract products
        products_df = pd.read_sql_query(
            "SELECT * FROM retail_products ORDER BY product_id",
            engine
        )

        if products_df.empty:
            raise ValueError("Product table is empty")

        products_file = (
            OUTPUT_FOLDER / f"retail_products_{timestamp}.csv"
        )

        products_df.to_csv(products_file, index=False)

        print("\nProduct extraction successful!")
        print("Product records:", len(products_df))
        print("Saved:", products_file)

        print("\nPostgreSQL ingestion completed successfully!")

    except Exception as error:
        print("PostgreSQL ingestion failed:", error)
        raise

    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    ingest_postgres()

