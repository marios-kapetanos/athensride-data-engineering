from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    desc,
    to_date,
    to_timestamp,
    unix_timestamp,
)

def main():
    spark = SparkSession.builder.appName("midterm-basic-batch").getOrCreate()

    rides = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/rides.csv")
    )

    rides_typed = (
        rides
        .withColumn("started_at", to_timestamp(col("started_at")))
        .withColumn("ended_at", to_timestamp(col("ended_at")))
    )

    rides_with_duration = rides_typed.withColumn(
        "duration_minutes",
        (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60
    )

    clean_rides = rides_with_duration.filter(
        (col("duration_minutes") >= 1)
        & (col("duration_minutes") <= 1440)
        & col("station_start").isNotNull()
        & col("station_end").isNotNull()
    )

    rides_per_day = (
        clean_rides
        .groupBy(to_date("started_at").alias("ride_date"))
        .agg(count("*").alias("rides"))
        .orderBy("ride_date")
    )

    avg_duration = (
        clean_rides
        .groupBy("user_type")
        .agg(avg("duration_minutes").alias("avg_duration_minutes"))
    )

    top_stations = (
        clean_rides
        .groupBy("station_start")
        .agg(count("*").alias("rides"))
        .orderBy(desc("rides"))
        .limit(10)
    )

    rides_per_day.write.mode("overwrite").parquet(
        "output/batch/rides_per_day"
    )

    avg_duration.write.mode("overwrite").parquet(
        "output/batch/avg_duration_by_user_type"
    )

    top_stations.write.mode("overwrite").parquet(
        "output/batch/top_start_stations"
    )

    rides_per_day.show()
    avg_duration.show()
    top_stations.show()

    spark.stop()


if __name__ == "__main__":
    main()