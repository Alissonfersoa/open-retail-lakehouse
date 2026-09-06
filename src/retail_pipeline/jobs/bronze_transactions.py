import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retail_pipeline.common.config import layer_path


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("created_at", TimestampType(), True),
])


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
    )

    parser.add_argument(
        "--batch-id",
        required=True,
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("bronze-transactions")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read
        .option("header", True)
        .schema(TRANSACTION_SCHEMA)
        .csv(args.input)
    )

    bronze = (
        df
        .withColumn(
            "ingestion_at",
            F.current_timestamp(),
        )
        .withColumn(
            "source_file",
            F.input_file_name(),
        )
        .withColumn(
            "batch_id",
            F.lit(args.batch_id),
        )
    )

    total = bronze.count()

    print(f"Bronze input records: {total:,}")

    output_path = (
        f"{layer_path('bronze', 'transactions')}"
        f"/batch_id={args.batch_id}"
    )

    (
        bronze
        .repartition(8)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Bronze written to: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()