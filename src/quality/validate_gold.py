
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_FOLDER = PROJECT_ROOT / "data" / "silver"
GOLD_FOLDER = PROJECT_ROOT / "data" / "gold"


def main():

    print("\n========== GOLD DATA QUALITY CHECKS STARTED ==========")

    spark = (
        SparkSession.builder
        .appName("Retail Gold Data Quality")
        .getOrCreate()
    )

    try:
        # Read Silver and Gold datasets
        silver_orders = spark.read.parquet(
            str(SILVER_FOLDER / "store_orders")
        )

        fact_sales = spark.read.parquet(
            str(GOLD_FOLDER / "fact_sales")
        )

        dim_customer = spark.read.parquet(
            str(GOLD_FOLDER / "dim_customer")
        )

        dim_product = spark.read.parquet(
            str(GOLD_FOLDER / "dim_product")
        )

        dim_date = spark.read.parquet(
            str(GOLD_FOLDER / "dim_date")
        )

        # 1. Check duplicate order lines
        duplicates = (
            silver_orders
            .groupBy("order_line_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        print("\n1. DUPLICATE ORDER LINES:", duplicates)

        # 2. Check missing and unknown dimension keys
        missing_customer_keys = fact_sales.filter(
            col("customer_key").isNull()
        ).count()

        missing_product_keys = fact_sales.filter(
            col("product_key").isNull()
        ).count()

        unknown_customer_keys = fact_sales.filter(
            col("customer_key") == 0
        ).count()

        unknown_product_keys = fact_sales.filter(
            col("product_key") == 0
        ).count()

        print("\n2. MISSING CUSTOMER KEYS:", missing_customer_keys)
        print("   MISSING PRODUCT KEYS:", missing_product_keys)
        print("   UNKNOWN CUSTOMER KEYS:", unknown_customer_keys)
        print("   UNKNOWN PRODUCT KEYS:", unknown_product_keys)

        # Check whether fact keys exist in their dimensions
        orphan_customers = (
            fact_sales.select("customer_key").distinct()
            .join(
                dim_customer.select("customer_key").distinct(),
                on="customer_key",
                how="left_anti"
            )
            .count()
        )

        orphan_products = (
            fact_sales.select("product_key").distinct()
            .join(
                dim_product.select("product_key").distinct(),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        orphan_dates = (
            fact_sales.select("date_key").distinct()
            .join(
                dim_date.select("date_key").distinct(),
                on="date_key",
                how="left_anti"
            )
            .count()
        )

        print("\n   ORPHAN CUSTOMER KEYS:", orphan_customers)
        print("   ORPHAN PRODUCT KEYS:", orphan_products)
        print("   ORPHAN DATE KEYS:", orphan_dates)

        # 3. Check invalid quantities and amounts
        invalid_quantities = fact_sales.filter(
            col("quantity").isNull() | (col("quantity") <= 0)
        ).count()

        invalid_amounts = fact_sales.filter(
            col("order_amount").isNull() | (col("order_amount") < 0)
        ).count()

        print("\n3. INVALID QUANTITIES:", invalid_quantities)
        print("   INVALID ORDER AMOUNTS:", invalid_amounts)

        # 4. Compare Silver and Gold record counts
        silver_count = silver_orders.count()
        gold_count = fact_sales.count()

        print("\n4. SILVER RECORD COUNT:", silver_count)
        print("   GOLD RECORD COUNT:", gold_count)

        count_match = silver_count == gold_count

        # 5. Compare Silver and Gold revenue
        silver_revenue = (
            silver_orders
            .agg(spark_sum("order_amount"))
            .first()[0]
        )

        gold_revenue = (
            fact_sales
            .agg(spark_sum("order_amount"))
            .first()[0]
        )

        print("\n5. SILVER REVENUE:", silver_revenue)
        print("   GOLD REVENUE:", gold_revenue)

        revenue_match = (
            silver_revenue is not None
            and gold_revenue is not None
            and abs(float(silver_revenue) - float(gold_revenue)) < 0.01
        )

        # Final quality results
        checks = {
            "No duplicate order lines": duplicates == 0,
            "No missing dimension keys": (
                missing_customer_keys == 0
                and missing_product_keys == 0
            ),
            "No orphan dimension keys": (
                orphan_customers == 0
                and orphan_products == 0
                and orphan_dates == 0
            ),
            "No invalid quantities": invalid_quantities == 0,
            "No invalid order amounts": invalid_amounts == 0,
            "Silver and Gold counts match": count_match,
            "Silver and Gold revenue match": revenue_match,
        }

        print("\n========== QUALITY CHECK SUMMARY ==========")

        for check_name, passed in checks.items():
            status = "PASS" if passed else "FAIL"
            print(f"{status}: {check_name}")

        print("\nUnknown customer records:", unknown_customer_keys)
        print("Unknown product records:", unknown_product_keys)

        if all(checks.values()):
            print("\nOVERALL RESULT: ALL CHECKS PASSED")
        else:
            print("\nOVERALL RESULT: SOME CHECKS FAILED")

            failed_checks = [
                name
                for name, passed in checks.items()
                if not passed
            ]

            raise RuntimeError(
                f"Gold data quality validation failed: {failed_checks}"
    )
        print("\n========== GOLD DATA QUALITY CHECKS COMPLETED ==========")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
