from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
  spark = (
    SparkSession.builder
    .appName("spark-smoke-test")
    .getOrCreate()
  )
  spark.sparkContext.setLogLevel("WARN")

  print("=" * 60)
  print("SPARK SMOKE TEST")
  print("=" * 60)

  print(f"Spark version: {spark.version}")
  print(f"Spark master: {spark.sparkContext.master}")
  print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

  df = spark.range(
    start=0,
    end=1_000_000,
    step=1,
    numPartitions=8,
  )
  result = (
    df
    .withColumn("group_id", F.col("id") % 10)
    .groupBy("group_id")
    .count()
    .orderBy("group_id")
  )
  result.show()
  total = df.count()
  print(f"Total records processed: {total:,}")

  spark.stop()

if __name__ == "__main__":
  main()