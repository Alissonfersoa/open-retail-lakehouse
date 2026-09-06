from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_pipeline.common.config import layer_path


def main():

    spark = (
        SparkSession.builder
        .appName("gold-transactions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.parquet(
        layer_path(
            "silver",
            "transactions",
        )
    )

    sales_daily = (
        silver
        .groupBy("transaction_date")
        .agg(
            F.countDistinct(
                "transaction_id"
            ).alias("total_orders"),

            F.countDistinct(
                "customer_id"
            ).alias("unique_customers"),

            F.round(
                F.sum("total_amount"),
                2,
            ).alias("revenue"),

            F.round(
                F.avg("total_amount"),
                2,
            ).alias("avg_ticket"),
        )
    )

    product_performance = (
        silver
        .groupBy("product_id")
        .agg(
            F.sum("quantity")
            .alias("quantity_sold"),

            F.round(
                F.sum("total_amount"),
                2,
            ).alias("revenue"),

            F.countDistinct(
                "transaction_id"
            ).alias("transactions"),
        )
    )

    state_performance = (
        silver
        .groupBy("state")
        .agg(
            F.countDistinct(
                "transaction_id"
            ).alias("orders"),

            F.countDistinct(
                "customer_id"
            ).alias("customers"),

            F.round(
                F.sum("total_amount"),
                2,
            ).alias("revenue"),
        )
    )

    sales_daily.write \
        .mode("overwrite") \
        .parquet(
            layer_path(
                "gold",
                "sales_daily",
            )
        )

    product_performance.write \
        .mode("overwrite") \
        .parquet(
            layer_path(
                "gold",
                "product_performance",
            )
        )

    state_performance.write \
        .mode("overwrite") \
        .parquet(
            layer_path(
                "gold",
                "state_performance",
            )
        )

    print("Gold marts created successfully.")

    spark.stop()


if __name__ == "__main__":
    main()