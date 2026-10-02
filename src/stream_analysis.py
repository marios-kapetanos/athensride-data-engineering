from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, to_timestamp, window
from pyspark.sql.types import StringType, StructField, StructType


RIDE_SCHEMA = StructType(
    [
        StructField("ride_id", StringType()),
        StructField("bike_id", StringType()),
        StructField("station_start", StringType()),
        StructField("station_end", StringType()),
        StructField("started_at", StringType()),
        StructField("ended_at", StringType()),
        StructField("user_type", StringType()),
    ]
)


def main():
    spark = SparkSession.builder.appName("midterm-basic-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "rides-live")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), RIDE_SCHEMA).alias("data"))
        .select("data.*")
    )

    rides = (
        parsed
        .withColumn("started_at_ts", to_timestamp(col("started_at")))
        .filter(col("started_at_ts").isNotNull())
    )

    counts = (
        rides
        .withWatermark("started_at_ts", "10 minutes")
        .groupBy(window(col("started_at_ts"), "5 minutes"))
        .agg(count("*").alias("ride_count"))
        .orderBy("window")
    )

    query = (
        counts.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", "output/checkpoints/rides-live")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()