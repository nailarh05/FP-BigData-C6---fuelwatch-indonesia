"""
Feature Engineering Module
---------------------------
Computes derived features for ML models from the aggregated streams.
Called as a batch job after streaming ETL has populated the aggregate tables.

Features produced per (city, window):
  - fuel_price_delta       : % change in fuel price vs previous window
  - traffic_index          : normalized congestion (0–1)
  - avg_vehicle_speed      : km/h
  - public_transport_usage : total ridership (normalized)
  - weather_score          : 0–100 (100 = clear sky)
  - time_of_day            : hour 0–23
  - holiday_flag           : 1 if national holiday
  - day_of_week            : 0=Monday … 6=Sunday
  - mobility_composite     : weighted composite mobility index
"""

import os
from datetime import date

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fuelwatch:fuelwatch_secret@localhost:5432/fuelwatch")

# Indonesian national holidays (static list — update annually)
NATIONAL_HOLIDAYS_2026 = {
    date(2026, 1, 1), date(2026, 3, 22), date(2026, 3, 29),
    date(2026, 3, 31), date(2026, 4, 1), date(2026, 5, 1),
    date(2026, 5, 14), date(2026, 5, 24), date(2026, 6, 1),
    date(2026, 8, 17), date(2026, 12, 25),
}

HOLIDAY_DATES_STR = [d.strftime("%Y-%m-%d") for d in NATIONAL_HOLIDAYS_2026]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FuelWatch-FeatureEngineering")
        .getOrCreate()
    )


def load_from_postgres(spark: SparkSession, table: str):
    jdbc_url = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")
    return spark.read.jdbc(
        url=jdbc_url,
        table=table,
        properties={
            "driver": "org.postgresql.Driver",
            "user": "fuelwatch",
            "password": "fuelwatch_secret",
        },
    )


def compute_fuel_delta(fuel_df):
    """Compute price delta (%) compared to previous 5-min window per city & fuel_type."""
    window_spec = Window.partitionBy("city", "fuel_type").orderBy("window_start")
    return (
        fuel_df
        .withColumn("prev_avg_price", F.lag("avg_price", 1).over(window_spec))
        .withColumn(
            "fuel_price_delta",
            F.when(
                F.col("prev_avg_price").isNotNull() & (F.col("prev_avg_price") > 0),
                (F.col("avg_price") - F.col("prev_avg_price")) / F.col("prev_avg_price") * 100,
            ).otherwise(0.0),
        )
    )


def normalize_col(df, col_name: str, new_name: str, min_val: float, max_val: float):
    return df.withColumn(
        new_name,
        F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(1.0),
                (F.col(col_name) - min_val) / (max_val - min_val),
            ),
        ),
    )


def build_feature_table(spark: SparkSession):
    fuel_df = compute_fuel_delta(load_from_postgres(spark, "fuel_price_agg"))
    traffic_df = load_from_postgres(spark, "traffic_agg")
    weather_df = load_from_postgres(spark, "weather_agg")
    transport_df = load_from_postgres(spark, "transport_agg")

    # Aggregate fuel to one row per (city, window_start) using Pertamax as benchmark
    fuel_city = (
        fuel_df
        .filter(F.col("fuel_type") == "Pertamax")
        .select("city", "window_start", "avg_price", "fuel_price_delta")
    )

    # Join all sources on city + window_start
    features = (
        fuel_city.alias("f")
        .join(
            traffic_df.select("city", "window_start", "avg_congestion", "avg_speed", "avg_mobility_index").alias("t"),
            on=["city", "window_start"],
            how="left",
        )
        .join(
            weather_df.select("city", "window_start", "avg_weather_score", "avg_temperature", "avg_rain_1h").alias("w"),
            on=["city", "window_start"],
            how="left",
        )
        .join(
            transport_df.select("city", "window_start", "total_ridership", "avg_load_factor").alias("tr"),
            on=["city", "window_start"],
            how="left",
        )
    )

    # Time features
    features = (
        features
        .withColumn("time_of_day", F.hour("window_start"))
        .withColumn("day_of_week", F.dayofweek("window_start") - 1)
        .withColumn(
            "holiday_flag",
            F.date_format("window_start", "yyyy-MM-dd").isin(HOLIDAY_DATES_STR).cast("int"),
        )
    )

    # Normalize numeric features
    features = normalize_col(features, "avg_congestion", "traffic_index", 0, 100)
    features = normalize_col(features, "avg_weather_score", "weather_score_norm", 0, 100)
    features = normalize_col(features, "total_ridership", "transport_usage_norm", 0, 50_000)

    # Composite mobility index: higher score = more mobile
    # Formula: (1 - traffic_index) * 0.4 + weather_score_norm * 0.3 + transport_usage_norm * 0.2 + (1 - fuel_price_delta/100) * 0.1
    features = features.withColumn(
        "mobility_composite",
        F.round(
            (1 - F.col("traffic_index")) * 0.40
            + F.col("weather_score_norm") * 0.30
            + F.col("transport_usage_norm") * 0.20
            + F.greatest(F.lit(0.0), (1 - F.col("fuel_price_delta") / 100)) * 0.10,
            4,
        ),
    )

    # Fill nulls
    feature_cols = [
        "fuel_price_delta", "traffic_index", "avg_speed", "transport_usage_norm",
        "weather_score_norm", "avg_temperature", "avg_rain_1h",
    ]
    fill_map = {c: 0.0 for c in feature_cols}
    features = features.fillna(fill_map)

    return features.select(
        "city", "window_start",
        "avg_price", "fuel_price_delta",
        "traffic_index", "avg_speed", "avg_mobility_index",
        "transport_usage_norm", "avg_load_factor",
        "weather_score_norm", "avg_temperature", "avg_rain_1h",
        "time_of_day", "day_of_week", "holiday_flag",
        "mobility_composite",
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    features = build_feature_table(spark)
    features.show(20, truncate=False)

    jdbc_url = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")
    (
        features.write
        .jdbc(
            url=jdbc_url,
            table="feature_store",
            mode="append",
            properties={
                "driver": "org.postgresql.Driver",
                "user": "fuelwatch",
                "password": "fuelwatch_secret",
            },
        )
    )
    print(f"Wrote {features.count()} feature rows to feature_store")
    spark.stop()


if __name__ == "__main__":
    main()
