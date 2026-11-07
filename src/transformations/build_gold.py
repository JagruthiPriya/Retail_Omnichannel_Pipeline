
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    date_format,
    date_add,
    lit,
    row_number,
    coalesce
)
from pyspark.sql.window import Window

# Find project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Silver input folder
SILVER_FOLDER = PROJECT_ROOT / "data" / "silver"

# Gold output folder
GOLD_FOLDER = PROJECT_ROOT / "data" / "gold"

# Create Gold folder
GOLD_FOLDER.mkdir(parents=True, exist_ok=True)


def create_customer_dimension(spark):

    print("\n========== BUILDING CUSTOMER DIMENSION ==========")

    # Read cleaned customer data from Silver
    customers_df = spark.read.parquet(
        str(SILVER_FOLDER / "retail_customers")
    )

    # Create a unique customer key
    window_spec = Window.orderBy("customer_id")

    dim_customer = (
        customers_df
        .withColumn(
            "customer_key",
            row_number().over(window_spec)
        )
        .select(
            "customer_key",
            "customer_id",
            "customer_name",
            "email",
            "city",
            "state"
        )
    )

    # Add SCD Type 2 tracking columns
    dim_customer = (
        dim_customer
        .withColumn("effective_start_date", lit("2026-11-07").cast("date"))
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
    )

    print("Customer dimension records:", dim_customer.count())

    dim_customer.show(truncate=False)

        # Add an Unknown Customer record for unmatched sales
    unknown_customer = dim_customer.limit(1).select(
        lit(0).alias("customer_key"),
        lit(-1).alias("customer_id"),
        lit("Unknown").alias("customer_name"),
        lit("unknown").alias("email"),
        lit("Unknown").alias("city"),
        lit("Unknown").alias("state"),
        lit("2026-11-07").cast("date").alias("effective_start_date"),
        lit(None).cast("date").alias("effective_end_date"),
        lit(True).alias("is_current")
    )

    dim_customer = dim_customer.unionByName(unknown_customer)

    print("Customer dimension including Unknown:", dim_customer.count())

    return dim_customer


def create_product_dimension(spark):

    print("\n========== BUILDING PRODUCT DIMENSION ==========")

    # Read cleaned product data from Silver
    products_df = spark.read.parquet(
        str(SILVER_FOLDER / "retail_products")
    )

    # Generate a unique product key
    window_spec = Window.orderBy("product_id")

    dim_product = (
        products_df
        .withColumn(
            "product_key",
            row_number().over(window_spec)
        )
        .select(
            "product_key",
            "product_id",
            "product_name",
            "category",
            "unit_price"
        )
    )

    # Add SCD Type 2 tracking columns
    dim_product = (
        dim_product
        .withColumn(
            "effective_start_date",
            lit("2026-11-07").cast("date")
        )
        .withColumn(
            "effective_end_date",
            lit(None).cast("date")
        )
        .withColumn("is_current", lit(True))
    )

   
    # Add an Unknown Product record for unmatched sales
    unknown_product = dim_product.limit(1).select(
        lit(0).alias("product_key"),
        lit(-1).alias("product_id"),
        lit("Unknown").alias("product_name"),
        lit("Unknown").alias("category"),
        lit(0.0).alias("unit_price"),
        lit("2026-11-07").cast("date").alias("effective_start_date"),
        lit(None).cast("date").alias("effective_end_date"),
        lit(True).alias("is_current")
    )

    # Combine original products with Unknown Product
    dim_product = dim_product.unionByName(unknown_product)

    print("Product dimension including Unknown:", dim_product.count())

    dim_product.show(truncate=False)

    return dim_product


def create_date_dimension(spark):

    print("\n========== BUILDING DATE DIMENSION ==========")

    # Read store orders from Silver
    store_df = spark.read.parquet(
        str(SILVER_FOLDER / "store_orders")
    )

    # Get unique order dates
    dates_df = (
        store_df
        .select(col("order_date").cast("date").alias("full_date"))
        .distinct()
        .filter(col("full_date").isNotNull())
    )

    # Generate a date key
    window_spec = Window.orderBy("full_date")

    dim_date = (
        dates_df
        .withColumn(
            "date_key",
            date_format(col("full_date"), "yyyyMMdd").cast("int")
        )
        .withColumn("year", year(col("full_date")))
        .withColumn("month", month(col("full_date")))
        .withColumn("day", dayofmonth(col("full_date")))
        .withColumn(
            "month_name",
            date_format(col("full_date"), "MMMM")
        )
        .withColumn(
            "day_name",
            date_format(col("full_date"), "EEEE")
        )
        .select(
            "date_key",
            "full_date",
            "year",
            "month",
            "month_name",
            "day",
            "day_name"
        )
        .orderBy("full_date")
    )

    print("Date dimension records:", dim_date.count())

    dim_date.show(truncate=False)

    return dim_date


def create_sales_fact(spark, dim_customer, dim_product, dim_date):

    print("\n========== BUILDING SALES FACT ==========")

    # Read cleaned and enriched store orders
    store_df = spark.read.parquet(
        str(SILVER_FOLDER / "store_orders")
    )

    # Join store orders with customer dimension
    fact_df = (
        store_df.alias("s")
        .join(
            dim_customer.alias("c"),
            col("s.customer_id") == col("c.customer_id"),
            "left"
        )
        .join(
            dim_product.alias("p"),
            col("s.product_id") == col("p.product_id"),
            "left"
        )
        .join(
            dim_date.alias("d"),
            col("s.order_date").cast("date") == col("d.full_date"),
            "left"
        )
    )

    # Select fact table columns
    fact_sales = fact_df.select(
        col("s.order_id"),
        col("s.order_line_id"),
        coalesce(col("c.customer_key"), lit(0)).alias("customer_key"),
        coalesce(col("p.product_key"), lit(0)).alias("product_key"),
        col("d.date_key"),
        col("s.quantity"),
        col("s.unit_price"),
        col("s.discount_amount"),
        col("s.order_amount"),
        col("s.channel"),
        col("s.store_id")
    )

    print("Total sales records:", fact_sales.count())

    print("\nSales fact preview:")
    fact_sales.show(truncate=False)

    return fact_sales


def save_gold(df, dataset_name):

    output_path = GOLD_FOLDER / dataset_name

    df.write.mode("overwrite").parquet(str(output_path))

    print(f"\n{dataset_name} saved successfully!")
    print("Records:", df.count())
    print("Location:", output_path)



def main():

    print("\n========== GOLD PIPELINE STARTED ==========")

    spark = (
        SparkSession.builder
        .appName("Retail Omnichannel Gold Layer")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # 1. Build customer dimension
        dim_customer = create_customer_dimension(spark)

        # 2. Build product dimension
        dim_product = create_product_dimension(spark)

        # 3. Build date dimension
        dim_date = create_date_dimension(spark)

        # 4. Build sales fact
        fact_sales = create_sales_fact(
            spark,
            dim_customer,
            dim_product,
            dim_date
        )

        # 5. Save all Gold datasets
        save_gold(dim_customer, "dim_customer")
        save_gold(dim_product, "dim_product")
        save_gold(dim_date, "dim_date")
        save_gold(fact_sales, "fact_sales")

        print("\n========== GOLD PIPELINE COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
