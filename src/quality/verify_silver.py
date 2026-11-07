

from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_FOLDER = PROJECT_ROOT / "data" / "silver"

spark = (
    SparkSession.builder
    .appName("Verify Silver Layer")
    .master("local[*]")
    .getOrCreate()
)

try:
    datasets = [
        "api_orders",
        "api_order_items",
        "store_orders",
        "retail_customers",
        "retail_products"
    ]

    print("\n========== SILVER RECORD COUNTS ==========")

    for name in datasets:
        df = spark.read.parquet(str(SILVER_FOLDER / name))
        print(name, ":", df.count())

    print("\n========== STORE DUPLICATE CHECK ==========")

    store_df = spark.read.parquet(
        str(SILVER_FOLDER / "store_orders")
    )

    duplicate_count = (
        store_df.count()
        - store_df.select("order_line_id").distinct().count()
    )

    print("Duplicate order lines:", duplicate_count)

    if duplicate_count != 0:
        raise ValueError("Silver contains duplicate order lines!")

    print("\n========== LATE ARRIVING ORDER CHECK ==========")

    late_order = store_df.filter(
        store_df.order_id == "S1005"
    )

    late_order_count = late_order.count()

    late_order.select(
        "order_id", "order_date", "updated_at", "order_amount"
    ).show(truncate=False)

    if late_order_count == 0:
        raise ValueError("Late-arriving order S1005 was not retained!")

    print("\n========== SILVER QUALITY CHECK SUMMARY ==========")
    print("PASS: No duplicate order lines")
    print("PASS: Late-arriving order S1005 retained")
    print("OVERALL RESULT: ALL CHECKS PASSED")

finally:
    spark.stop()
