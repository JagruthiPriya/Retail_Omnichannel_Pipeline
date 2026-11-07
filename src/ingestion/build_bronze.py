
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp


# 1. Find project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 2. Define raw data folders
API_FOLDER = PROJECT_ROOT / "data" / "raw" / "api"
CSV_FOLDER = PROJECT_ROOT / "data" / "raw" / "csv"
POSTGRES_FOLDER = PROJECT_ROOT / "data" / "raw" / "postgres"

# 3. Define Bronze output folder
BRONZE_FOLDER = PROJECT_ROOT / "data" / "bronze"

# Unique folder for this ingestion run
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

def get_latest_file(folder, pattern):
    """Return the latest file matching the given pattern."""

    files = list(folder.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No files found in {folder} matching {pattern}"
        )

    # Timestamped filenames sort chronologically
    return max(files, key=lambda file: file.stat().st_mtime)

def add_bronze_metadata(df):
    """Add source filename and ingestion timestamp."""
    return (
        df.withColumn("_source_file", input_file_name())
          .withColumn("_ingestion_timestamp", current_timestamp())
    )


def save_bronze(df, dataset_name):
    """Save dataset as Parquet in the Bronze layer."""

    output_path = BRONZE_FOLDER / dataset_name / f"run_id={RUN_ID}"

    df = add_bronze_metadata(df)

    df.write.mode("overwrite").parquet(str(output_path))

    print(f"\n{dataset_name} saved successfully!")
    print("Records:", df.count())
    print("Location:", output_path)

    return df


def main():

    print("========== BRONZE INGESTION STARTED ==========")

    # 4. Create Spark session
    spark = (
        SparkSession.builder
        .appName("Retail Omnichannel Bronze Ingestion")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # 5. Find the latest raw files

        api_files = [
            str(get_latest_file(API_FOLDER, "api_orders_*.json"))
        ]

        store_files = [
            str(get_latest_file(CSV_FOLDER, "store_orders.csv"))
        ]

        customer_files = [
            str(get_latest_file(
                POSTGRES_FOLDER,
                "retail_customers_*.csv"
            ))
        ]

        product_files = [
            str(get_latest_file(
                POSTGRES_FOLDER,
                "retail_products_*.csv"
            ))
        ]

        print("\nLatest raw files selected:")
        print("API:", api_files[0])
        print("Store orders:", store_files[0])
        print("Customers:", customer_files[0])
        print("Products:", product_files[0])

        # 6. Read API JSON data
        print("\nReading API JSON data...")

        api_df = (
            spark.read
            .option("multiLine", "true")
            .json(api_files)
        )

        api_bronze = save_bronze(api_df, "api_orders")

        # 7. Read store CSV data
        print("\nReading store CSV data...")

        store_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(store_files)
        )

        store_bronze = save_bronze(store_df, "store_orders")

        # 8. Read PostgreSQL customer data
        print("\nReading PostgreSQL customer data...")

        customers_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(customer_files)
        )

        customers_bronze = save_bronze(
            customers_df, "retail_customers"
        )

        # 9. Read PostgreSQL product data
        print("\nReading PostgreSQL product data...")

        products_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(product_files)
        )

        products_bronze = save_bronze(
            products_df, "retail_products"
        )

        # 10. Display sample records
        print("\n========== BRONZE DATA PREVIEW ==========")

        print("\nAPI orders:")
        api_bronze.show(3, truncate=False)

        print("\nStore orders:")
        store_bronze.show(5, truncate=False)

        print("\nRetail customers:")
        customers_bronze.show(5, truncate=False)

        print("\nRetail products:")
        products_bronze.show(5, truncate=False)

        print("\n========== BRONZE INGESTION COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
