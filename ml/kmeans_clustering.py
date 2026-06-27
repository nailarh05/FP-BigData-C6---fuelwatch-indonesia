"""
K-Means City Clustering
-----------------------
Segments cities into 3 impact clusters based on their response to fuel price changes:
  Cluster 0: High Impact   — large mobility drop after BBM price increase
  Cluster 1: Moderate      — medium impact, partial modal shift
  Cluster 2: Low Impact    — resilient cities (good public transport alternatives)

Uses PySpark MLlib for distributed clustering.
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fuelwatch:fuelwatch_secret@localhost:5432/fuelwatch")
MODEL_DIR = Path(__file__).parent / "saved_models"
MODEL_DIR.mkdir(exist_ok=True)

CLUSTER_LABELS = {
    0: "high_impact",
    1: "moderate_impact",
    2: "low_impact",
}

FEATURE_COLS = [
    "avg_fuel_price_delta",       # avg % change in fuel price
    "avg_mobility_drop",          # avg drop in mobility after price increase
    "alt_transport_ratio",        # proportion of trips on public transport
    "avg_congestion_change",      # change in congestion after price increase
    "city_income_proxy",          # normalized GDP per capita proxy
]


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("FuelWatch-KMeans").getOrCreate()


def load_city_features(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """
    Load pre-computed city-level impact features from PostgreSQL.
    Each row = one city aggregate over a time window.
    """
    jdbc_url = DATABASE_URL.replace("postgresql://", "jdbc:postgresql://")
    return spark.read.jdbc(
        url=jdbc_url,
        table="city_impact_features",
        properties={
            "driver": "org.postgresql.Driver",
            "user": "fuelwatch",
            "password": "fuelwatch_secret",
        },
    )


def train_kmeans(spark: SparkSession, df=None, k: int = 3, seed: int = 42):
    if df is None:
        df = load_city_features(spark)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withMean=True, withStd=True)

    assembled = assembler.transform(df)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)

    # Elbow method: try k=2..5 and pick optimal silhouette
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    best_silhouette = -1.0
    best_model = None

    for k_try in range(2, 6):
        km = KMeans(k=k_try, seed=seed, featuresCol="features", predictionCol="cluster")
        model = km.fit(scaled)
        preds = model.transform(scaled)
        sil = evaluator.evaluate(preds)
        print(f"  k={k_try} → silhouette={sil:.4f}")
        if sil > best_silhouette:
            best_silhouette = sil
            best_model = model
            k = k_try

    print(f"\nBest k={k}, silhouette={best_silhouette:.4f}")

    predictions = best_model.transform(scaled)
    best_model.write().overwrite().save(str(MODEL_DIR / "kmeans_cities"))

    return best_model, predictions


def assign_cluster_labels(predictions_df) -> pd.DataFrame:
    """
    Map integer cluster IDs to human-readable impact labels.
    Determine which cluster = 'high impact' by sorting on avg_mobility_drop desc.
    """
    pdf = predictions_df.toPandas()
    cluster_mobility = (
        pdf.groupby("cluster")["avg_mobility_drop"]
        .mean()
        .sort_values(ascending=False)
    )
    # Rank: highest mobility drop = high_impact
    rank_map = {cid: ["high_impact", "moderate_impact", "low_impact"][i]
                for i, cid in enumerate(cluster_mobility.index)}

    pdf["impact_level"] = pdf["cluster"].map(rank_map)
    return pdf[["city", "cluster", "impact_level"] + FEATURE_COLS]


def predict_city_cluster(city_features: dict) -> dict:
    """
    Predict cluster for a new city given its feature dict.
    Quick inference without Spark (uses saved cluster centroids).
    """
    model_path = MODEL_DIR / "kmeans_cities"
    if not model_path.exists():
        return {"error": "Model not trained yet"}

    spark = build_spark_session()
    model = KMeansModel.load(str(model_path))

    row_df = spark.createDataFrame([city_features])
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="raw_features")
    assembled = assembler.transform(row_df)

    # We'd need the saved scaler too in a real system; simplified here
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.linalg import Vectors

    centers = model.clusterCenters()
    feat_vec = np.array([city_features[c] for c in FEATURE_COLS])

    distances = [np.linalg.norm(feat_vec - center) for center in centers]
    cluster = int(np.argmin(distances))
    return {
        "predicted_cluster": cluster,
        "impact_level": CLUSTER_LABELS.get(cluster, "unknown"),
        "distances": {i: float(d) for i, d in enumerate(distances)},
    }


def generate_synthetic_city_data() -> pd.DataFrame:
    """Demo data for 6 Indonesian cities."""
    return pd.DataFrame({
        "city": ["Jakarta", "Surabaya", "Bandung", "Medan", "Makassar", "Semarang"],
        "avg_fuel_price_delta": [8.5, 7.2, 9.1, 10.3, 8.8, 7.9],
        "avg_mobility_drop": [12.3, 8.7, 15.2, 18.5, 11.1, 9.4],
        "alt_transport_ratio": [0.42, 0.28, 0.21, 0.15, 0.19, 0.25],
        "avg_congestion_change": [-5.2, -3.8, -7.1, -8.9, -4.5, -3.2],
        "city_income_proxy": [0.85, 0.70, 0.65, 0.60, 0.58, 0.67],
    })


if __name__ == "__main__":
    spark = build_spark_session()

    pdf = generate_synthetic_city_data()
    spark_df = spark.createDataFrame(pdf)

    model, predictions = train_kmeans(spark, df=spark_df)

    result = assign_cluster_labels(predictions)
    print("\nCity Clustering Results:")
    print(result[["city", "impact_level", "avg_mobility_drop"]].to_string(index=False))

    spark.stop()
