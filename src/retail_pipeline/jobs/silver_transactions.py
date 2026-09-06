from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_pipeline.common.config import layer_path


def main():

    spark = (
        SparkSession.builder
        .appName("silver-transactions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze_path = layer_path(
        "bronze",
        "transactions",
    )

    silver_path = layer_path(
        "silver",
        "transactions",
    )

    quarantine_path = layer_path(
        "quarantine",
        "transactions",
    )

    bronze = (
        spark.read
        .parquet(bronze_path)
        .cache()
    )

    input_count = bronze.count()

    validated = bronze.withColumn(
        "dq_reason",
        F.when(
            F.col("transaction_id").isNull(),
            "NULL_TRANSACTION_ID",
        )
        .when(
            F.col("customer_id").isNull(),
            "NULL_CUSTOMER_ID",
        )
        .when(
            F.col("product_id").isNull(),
            "NULL_PRODUCT_ID",
        )
        .when(
            F.col("quantity") <= 0,
            "INVALID_QUANTITY",
        )
        .when(
            F.col("unit_price") <= 0,
            "INVALID_PRICE",
        )
        .when(
            ~F.col("discount_pct").between(0, 0.50),
            "INVALID_DISCOUNT",
        )
    )

    invalid = validated.filter(
        F.col("dq_reason").isNotNull()
    )

    candidates = validated.filter(
        F.col("dq_reason").isNull()
    )

    window = (
        Window
        .partitionBy("transaction_id")
        .orderBy(F.col("created_at").desc())
    )

    ranked = candidates.withColumn(
        "row_number",
        F.row_number().over(window),
    )

    duplicates = (
        ranked
        .filter(F.col("row_number") > 1)
        .drop("row_number")
        .withColumn(
            "dq_reason",
            F.lit("DUPLICATE_TRANSACTION_ID"),
        )
    )

    valid = (
        ranked
        .filter(F.col("row_number") == 1)
        .drop("row_number", "dq_reason")
    )

    silver = (
        valid
        .withColumn(
            "gross_amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price"),
                2,
            ),
        )
        .withColumn(
            "total_amount",
            F.round(
                F.col("quantity")
                * F.col("unit_price")
                * (1 - F.col("discount_pct")),
                2,
            ),
        )
        .withColumn(
            "transaction_year",
            F.year("transaction_date"),
        )
        .withColumn(
            "transaction_month",
            F.month("transaction_date"),
        )
    )

    rejected = invalid.unionByName(
        duplicates,
        allowMissingColumns=True,
    )

    output_count = silver.count()
    rejected_count = rejected.count()

    print("=" * 60)
    print("SILVER DATA QUALITY")
    print("=" * 60)

    print(f"Input records:    {input_count:,}")
    print(f"Output records:   {output_count:,}")
    print(f"Rejected records: {rejected_count:,}")

    (
        silver
        .repartition(
            8,
            "transaction_year",
            "transaction_month",
        )
        .write
        .mode("overwrite")
        .partitionBy(
            "transaction_year",
            "transaction_month",
        )
        .parquet(silver_path)
    )

    (
        rejected
        .write
        .mode("overwrite")
        .parquet(quarantine_path)
    )

    bronze.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()