"""
FuelWatch ETL Pipeline — Apache Spark Structured Streaming
----------------------------------------------------------
Reads from 4 Kafka topics, applies ETL + feature engineering,
joins streams in a 5-minute window, and writes to PostgreSQL + Redis.

Run via:
  spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
    processing/spark_streaming/etl_pipeline.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fuelwatch:fuelwatch_secret@localhost:5432/fuelwatch")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ── Schemas ──────────────────────────────────────────────────────────────────

FUEL_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("fuel_type", StringType()),
    StructField("price", DoubleType()),
    StructField("station", StringType()),
    StructField("source", StringType()),
    StructField("surge_event", BooleanType()),
])

TRAFFIC_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("road_id", StringType()),
    StructField("road_name", StringType()),
    StructField("congestion_level", DoubleType()),
    StructField("avg_speed", DoubleType()),
    StructField("free_flow_speed", DoubleType()),
    StructField("mobility_index", DoubleType()),
    StructField("source", StringType()),
])

WEATHER_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("weather_main", StringType()),
    StructField("rain_1h", DoubleType()),
    StructField("weather_score", DoubleType()),
    StructField("source", StringType()),
])

TRANSPORT_SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("transport_mode", StringType()),
    StructField("transport_code", StringType()),
    StructField("ridership", IntegerType()),
    StructField("capacity", IntegerType()),
    StructField("load_factor_pct", DoubleType()),
    StructField("source", StringType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FuelWatch-ETL-Pipeline")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/fuelwatch_checkpoint")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str, schema: StructType):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
        .select(F.from_json(F.col("json_str"), schema).alias("data"), "kafka_ts")
        .select("data.*", "kafka_ts")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withWatermark("event_time", "2 minutes")
    )


def process_fuel_stream(df):
    """ETL + feature engineering for fuel price stream."""
    return (
        df
        .filter(F.col("price") > 0)
        .filter(F.col("city").isNotNull())
        # Normalize: compute price category
        .withColumn("price_category",
            F.when(F.col("price") < 10_000, "subsidi")
             .when(F.col("price") < 14_000, "ron90")
             .when(F.col("price") < 16_000, "ron95")
             .otherwise("premium")
        )
        # Time features
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("day_of_week", F.dayofweek("event_time"))
        .withColumn("is_weekend", (F.dayofweek("event_time").isin(1, 7)).cast("int"))
        # Aggregate: average price per city per fuel type in 5-min tumbling window
        .groupBy(
            F.window("event_time", "5 minutes"),
            "city",
            "fuel_type",
            "price_category",
        )
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.count("*").alias("sample_count"),
            F.avg("is_weekend").alias("is_weekend"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "city", "fuel_type", "price_category",
            "avg_price", "min_price", "max_price", "sample_count", "is_weekend",
        )
    )


def process_traffic_stream(df):
    """ETL + feature engineering for traffic stream."""
    return (
        df
        .filter(F.col("congestion_level").between(0, 100))
        .filter(F.col("avg_speed") > 0)
        .withColumn("traffic_severity",
            F.when(F.col("congestion_level") > 80, "critical")
             .when(F.col("congestion_level") > 60, "heavy")
             .when(F.col("congestion_level") > 40, "moderate")
             .otherwise("light")
        )
        .groupBy(
            F.window("event_time", "5 minutes"),
            "city",
            "traffic_severity",
        )
        .agg(
            F.avg("congestion_level").alias("avg_congestion"),
            F.avg("avg_speed").alias("avg_speed"),
            F.avg("mobility_index").alias("avg_mobility_index"),
            F.count("road_id").alias("roads_monitored"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "city", "traffic_severity",
            "avg_congestion", "avg_speed", "avg_mobility_index", "roads_monitored",
        )
    )


def process_weather_stream(df):
    return (
        df
        .filter(F.col("weather_score").between(0, 100))
        .groupBy(
            F.window("event_time", "5 minutes"),
            "city",
        )
        .agg(
            F.avg("weather_score").alias("avg_weather_score"),
            F.avg("temperature").alias("avg_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("rain_1h").alias("avg_rain_1h"),
            F.first("weather_main").alias("dominant_weather"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "city", "avg_weather_score", "avg_temperature",
            "avg_humidity", "avg_rain_1h", "dominant_weather",
        )
    )


def process_transport_stream(df):
    return (
        df
        .filter(F.col("ridership") >= 0)
        .groupBy(
            F.window("event_time", "5 minutes"),
            "city",
        )
        .agg(
            F.sum("ridership").alias("total_ridership"),
            F.avg("load_factor_pct").alias("avg_load_factor"),
            F.count("transport_mode").alias("active_modes"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "city", "total_ridership", "avg_load_factor", "active_modes",
        )
    )


def write_to_postgres(df, table: str, checkpoint: str):
    jdbc_url = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")
    pg_props = {
        "driver": "org.postgresql.Driver",
        "user": "fuelwatch",
        "password": "fuelwatch_secret",
    }

    def write_batch(batch_df, _epoch_id):
        if batch_df.count() == 0:
            return
        (
            batch_df.write
            .jdbc(url=jdbc_url, table=table, mode="append", properties=pg_props)
        )

    return (
        df.writeStream
        .outputMode("update")
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_to_console(df, output_mode="update"):
    return (
        df.writeStream
        .outputMode(output_mode)
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read all streams
    fuel_raw = read_kafka_stream(spark, "fuel-price-stream", FUEL_SCHEMA)
    traffic_raw = read_kafka_stream(spark, "traffic-stream", TRAFFIC_SCHEMA)
    weather_raw = read_kafka_stream(spark, "weather-stream", WEATHER_SCHEMA)
    transport_raw = read_kafka_stream(spark, "transport-stream", TRANSPORT_SCHEMA)

    # Process
    fuel_processed = process_fuel_stream(fuel_raw)
    traffic_processed = process_traffic_stream(traffic_raw)
    weather_processed = process_weather_stream(weather_raw)
    transport_processed = process_transport_stream(transport_raw)

    # Write to PostgreSQL
    q1 = write_to_postgres(fuel_processed, "fuel_price_agg", "/tmp/cp_fuel")
    q2 = write_to_postgres(traffic_processed, "traffic_agg", "/tmp/cp_traffic")
    q3 = write_to_postgres(weather_processed, "weather_agg", "/tmp/cp_weather")
    q4 = write_to_postgres(transport_processed, "transport_agg", "/tmp/cp_transport")

    # Wait for all queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
