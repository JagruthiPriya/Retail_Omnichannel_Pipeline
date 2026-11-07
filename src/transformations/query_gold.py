
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_FOLDER = PROJECT_ROOT / "data" / "gold"


def main():

    print("\n========== GOLD SQL REPORTING STARTED ==========")

    spark = (
        SparkSession.builder
        .appName("Retail Gold SQL Reporting")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        # Read Gold Parquet datasets
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

        # Register temporary SQL tables
        fact_sales.createOrReplaceTempView("fact_sales")
        dim_customer.createOrReplaceTempView("dim_customer")
        dim_product.createOrReplaceTempView("dim_product")
        dim_date.createOrReplaceTempView("dim_date")

        # REPORT 1: Overall sales
        print("\n========== REPORT 1: OVERALL SALES ==========")

        spark.sql("""
            SELECT
                COUNT(*) AS total_order_lines,
                SUM(quantity) AS total_units_sold,
                SUM(order_amount) AS total_revenue,
                ROUND(AVG(order_amount), 2) AS average_line_amount
            FROM fact_sales
        """).show(truncate=False)

        # REPORT 2: Sales by product
        print("\n========== REPORT 2: SALES BY PRODUCT ==========")

        spark.sql("""
            SELECT
                p.product_name,
                p.category,
                SUM(f.quantity) AS units_sold,
                SUM(f.order_amount) AS total_revenue
            FROM fact_sales f
            LEFT JOIN dim_product p
                ON f.product_key = p.product_key
            GROUP BY p.product_name, p.category
            ORDER BY total_revenue DESC
        """).show(truncate=False)

        # REPORT 3: Sales by date
        print("\n========== REPORT 3: SALES BY DATE ==========")

        spark.sql("""
            SELECT
                d.full_date,
                SUM(f.quantity) AS units_sold,
                SUM(f.order_amount) AS total_revenue
            FROM fact_sales f
            LEFT JOIN dim_date d
                ON f.date_key = d.date_key
            GROUP BY d.full_date
            ORDER BY d.full_date
        """).show(truncate=False)

        # REPORT 4: Sales by store
        print("\n========== REPORT 4: SALES BY STORE ==========")

        spark.sql("""
            SELECT
                store_id,
                COUNT(*) AS total_order_lines,
                SUM(quantity) AS units_sold,
                SUM(order_amount) AS total_revenue
            FROM fact_sales
            GROUP BY store_id
            ORDER BY total_revenue DESC
        """).show(truncate=False)

        print("\n========== GOLD SQL REPORTING COMPLETED ==========")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
