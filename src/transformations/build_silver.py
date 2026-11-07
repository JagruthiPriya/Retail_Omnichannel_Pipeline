from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import lower
from pathlib import Path
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    lit,
    to_date,
    input_file_name
)

# Find the main project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Bronze input folder
BRONZE_FOLDER = PROJECT_ROOT / "data" / "bronze"

def get_latest_bronze_path(dataset_name):

    dataset_folder = BRONZE_FOLDER / dataset_name

    run_folders = list(dataset_folder.glob("run_id=*"))

    if not run_folders:
        raise FileNotFoundError(
            f"No Bronze runs found for {dataset_name}"
        )

    latest_run = max(
    run_folders,
    key=lambda folder: folder.stat().st_mtime
)

    return str(latest_run)

# Silver output folder
SILVER_FOLDER = PROJECT_ROOT / "data" / "silver"

# Create Silver folder if it does not exist
SILVER_FOLDER.mkdir(parents=True, exist_ok=True)


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Retail Omnichannel Silver Transformation")
        .master("local[*]")
        .getOrCreate()
    )

    return spark


def read_bronze_data(spark):

    print("\n========== READING BRONZE DATA ==========")

        # Read only the latest Bronze run for each dataset

    api_df = spark.read.parquet(
        get_latest_bronze_path("api_orders")
    )

    store_df = spark.read.parquet(
        get_latest_bronze_path("store_orders")
    )

    customers_df = spark.read.parquet(
        get_latest_bronze_path("retail_customers")
    )

    products_df = spark.read.parquet(
        get_latest_bronze_path("retail_products")
    )
    print("\nBronze record counts:")
    print("API records:", api_df.count())
    print("Store order records:", store_df.count())
    print("Customer records:", customers_df.count())
    print("Product records:", products_df.count())

    return api_df, store_df, customers_df, products_df

def flatten_api_data(api_df):

    print("\n========== FLATTENING API DATA ==========")

    # Convert the carts array into separate rows
    carts_df = api_df.select(
        explode(col("carts")).alias("cart")
    )

    # Select order-level information
    orders_df = carts_df.select(
        col("cart.id").alias("order_id"),
        col("cart.userId").alias("customer_id"),
        col("cart.total").alias("total_amount"),
        col("cart.discountedTotal").alias("discounted_amount"),
        col("cart.totalProducts").alias("total_products"),
        col("cart.totalQuantity").alias("total_quantity")
    )

    print("Total API orders:", orders_df.count())

    # Convert each cart's products array into separate rows
    products_df = carts_df.select(
        col("cart.id").alias("order_id"),
        col("cart.userId").alias("customer_id"),
        explode(col("cart.products")).alias("product")
    )

    # Select product-level information
    order_items_df = products_df.select(
        col("order_id"),
        col("customer_id"),
        col("product.id").alias("product_id"),
        col("product.title").alias("product_name"),
        col("product.quantity").alias("quantity"),
        col("product.price").alias("unit_price"),
        col("product.total").alias("line_total"),
        col("product.discountPercentage").alias("discount_percentage"),
        col("product.discountedTotal").alias("discounted_line_total")
    )

    print("Total API order items:", order_items_df.count())

    return orders_df, order_items_df


def clean_store_orders(store_df):

    print("\n========== CLEANING STORE ORDERS ==========")

    print("Records before cleaning:", store_df.count())

    # Remove rows with missing important values
    cleaned_df = store_df.dropna(
        subset=[
            "order_id",
            "order_line_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_date"
        ]
    )

    # Keep valid quantities and prices
    cleaned_df = cleaned_df.filter(
        (col("quantity") > 0) &
        (col("unit_price") >= 0) &
        (col("discount_amount") >= 0)
    )

    # Keep the latest record for each order line
    window_spec = (
        Window.partitionBy("order_line_id")
        .orderBy(col("updated_at").desc())
    )

    cleaned_df = (
        cleaned_df
        .withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )

    # Calculate the net order-line amount
    cleaned_df = cleaned_df.withColumn(
        "order_amount",
        (col("quantity") * col("unit_price")) - col("discount_amount")
    )

    print("Records after cleaning:", cleaned_df.count())

    return cleaned_df


def clean_customers(customers_df):

    print("\n========== CLEANING CUSTOMERS ==========")

    print("Customers before cleaning:", customers_df.count())

    cleaned_df = customers_df.dropna(
        subset=["customer_id", "customer_name", "email"]
    )

    # Remove duplicate customer IDs
    cleaned_df = cleaned_df.dropDuplicates(["customer_id"])

    # Standardize customer name and email
    cleaned_df = (
        cleaned_df
        .withColumn("customer_name", 
                    col("customer_name").cast("string"))
        .withColumn("email", 
                    col("email").cast("string"))
    )

    cleaned_df = cleaned_df.withColumn(
    "email",
    lower(col("email"))
)
    print("Customers after cleaning:", cleaned_df.count())

    return cleaned_df


def clean_products(products_df):

    print("\n========== CLEANING PRODUCTS ==========")

    print("Products before cleaning:", products_df.count())

    cleaned_df = products_df.dropna(
        subset=["product_id", "product_name", "unit_price"]
    )

    # Remove duplicate product IDs
    cleaned_df = cleaned_df.dropDuplicates(["product_id"])

    # Keep only valid prices
    cleaned_df = cleaned_df.filter(
        col("unit_price") >= 0
    )

    print("Products after cleaning:", cleaned_df.count())

    return cleaned_df


def enrich_store_orders(store_df, customers_df, products_df):

    print("\n========== ENRICHING STORE ORDERS ==========")

    # Join store orders with customer information
    enriched_df = (
        store_df.alias("s")
        .join(
            customers_df.alias("c"),
            col("s.customer_id") == col("c.customer_id"),
            "left"
        )
        .join(
            products_df.alias("p"),
            col("s.product_id") == col("p.product_id"),
            "left"
        )
        .select(
            col("s.*"),
            col("c.customer_name"),
            col("c.city"),
            col("c.state"),
            col("p.category"),
            col("p.unit_price").alias("master_unit_price")
        )
    )

    print("Enriched store records:", enriched_df.count())

    return enriched_df


def save_silver(df, dataset_name):

    output_path = SILVER_FOLDER / dataset_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"\n{dataset_name} saved successfully!")
    print("Records:", df.count())
    print("Location:", output_path)



def main():

    spark = create_spark_session()

    try:
        # 1. Read Bronze datasets
        api_df, store_df, customers_df, products_df = (
            read_bronze_data(spark)
        )

        # 2. Flatten API data
        api_orders_df, api_items_df = flatten_api_data(api_df)

        # 3. Clean store orders
        clean_store_df = clean_store_orders(store_df)

        # 4. Clean customers
        clean_customers_df = clean_customers(customers_df)

        # 5. Clean products
        clean_products_df = clean_products(products_df)

        # 6. Enrich store orders
        enriched_store_df = enrich_store_orders(
            clean_store_df,
            clean_customers_df,
            clean_products_df
        )

        # 7. Save Silver datasets
        save_silver(api_orders_df, "api_orders")
        save_silver(api_items_df, "api_order_items")
        save_silver(enriched_store_df, "store_orders")
        save_silver(clean_customers_df, "retail_customers")
        save_silver(clean_products_df, "retail_products")

        print("\n========== SILVER TRANSFORMATION COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
