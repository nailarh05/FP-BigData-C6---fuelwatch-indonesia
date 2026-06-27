"""
FuelWatch Spark Processor — PROCESSING LAYER (CPMK-2) + LAKEHOUSE (CPMK-3) + ANALYTICS (CPMK-4)

Kenapa Spark (justifikasi CPMK-2):
- In-memory DataFrame processing jauh lebih cepat dibanding loop pandas murni
  untuk agregasi berulang (groupBy city/period/hour) yang dijalankan tiap 5 menit.
- API MLlib (RandomForest, KMeans) terintegrasi langsung dengan DataFrame yang sama,
  tidak perlu pindah ke library lain untuk training model.
- Local mode (local[2]) dipakai supaya project tetap ringan dijalankan di laptop,
  tapi kode ini portable ke cluster Spark sungguhan tanpa ubah logika (cukup ganti master URL).

Pipeline tiap siklus (default 5 menit):
  BRONZE (Postgres bronze_traffic, hasil Kafka)
    -> SILVER  : cleaning, dedup, feature engineering           -> silver_traffic + Parquet
    -> GOLD    : agregasi (comparison, hourly, daily+estimasi)  -> gold_* tables
    -> ML #1   : RandomForest forecasting + evaluasi (accuracy/F1)
    -> ML #2   : KMeans clustering spasial (zona dampak BBM)
    -> ML #3   : Z-score anomaly detection (lonjakan kemacetan tak wajar)
"""
import os
import time
import json
import logging
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import redis
import schedule
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.clustering import KMeans

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [SPARK] %(message)s')

BBM_DATE        = os.getenv('BBM_EVENT_DATE', '2026-06-10')
DATA_LAKE_PATH  = os.getenv('DATA_LAKE_PATH', '/data')
LEVEL_LABELS    = {0: 'Lancar', 1: 'Ramai Lancar', 2: 'Padat', 3: 'Macet'}

# ── Asumsi estimasi dampak kuantitatif (didokumentasikan di README) ──
FUEL_PRICE_AFTER_IDR   = 16250   # Rp/liter, harga Pertamax pasca kenaikan 10 Jun 2026
ASSUMED_DAILY_KM       = 30      # asumsi jarak tempuh komuter harian rata-rata (km)
BASE_EFFICIENCY_KMPL   = 12      # efisiensi BBM saat free-flow (km/liter), mobil rata-rata
MAX_CONGESTION_PENALTY = 0.5     # di congestion_index=100%, konsumsi BBM naik s.d. +50%

def get_db():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'fuelwatch'),
        user=os.getenv('POSTGRES_USER', 'fuelwatch'),
        password=os.getenv('POSTGRES_PASSWORD', 'fuelwatch123'),
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
    )

def get_redis():
    return redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

def get_spark():
    return (
        SparkSession.builder
        .master('local[2]')
        .appName('fuelwatch-spark-processor')
        .config('spark.driver.memory', '1g')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.ui.showConsoleProgress', 'false')
        .getOrCreate()
    )

# ============================================================
# BRONZE -> SILVER
# ============================================================
def load_bronze(conn, spark):
    """Window read 60 hari terakhir — cukup untuk cover seed historis + live data,
    sekaligus mencegah full-table-scan tak terbatas saat tabel terus tumbuh."""
    pdf = pd.read_sql("""
        SELECT city, road_name, lat, lon, current_speed, free_flow_speed,
               congestion_index, period, recorded_at
        FROM bronze_traffic
        WHERE recorded_at >= NOW() - INTERVAL '60 days'
    """, conn)
    if pdf.empty:
        return None
    pdf['recorded_at'] = pd.to_datetime(pdf['recorded_at'])
    for col in ['lat', 'lon', 'current_speed', 'free_flow_speed', 'congestion_index']:
        pdf[col] = pdf[col].astype(float)
    return spark.createDataFrame(pdf)

def transform_silver(bronze_df):
    """SILVER: dedup, filter nilai tidak valid, tambah fitur waktu + label congestion_level."""
    return (
        bronze_df
        .dropDuplicates(['city', 'road_name', 'recorded_at'])
        .filter((F.col('congestion_index') >= 0) & (F.col('congestion_index') <= 100))
        .filter(F.col('current_speed') > 0)
        .withColumn('hour', F.hour('recorded_at'))
        .withColumn('day_of_week', F.dayofweek('recorded_at'))
        .withColumn('is_weekend', F.when(F.col('day_of_week').isin([1, 7]), 1).otherwise(0))
        .withColumn('congestion_level',
            F.when(F.col('congestion_index') < 20, 0)
             .when(F.col('congestion_index') < 40, 1)
             .when(F.col('congestion_index') < 60, 2)
             .otherwise(3))
    )

def write_silver(conn, silver_df):
    cols = ['city', 'road_name', 'lat', 'lon', 'current_speed', 'free_flow_speed',
            'congestion_index', 'congestion_level', 'period', 'hour', 'day_of_week',
            'is_weekend', 'recorded_at']
    pdf = silver_df.select(*cols).toPandas()
    if pdf.empty:
        return

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE silver_traffic")
        rows = [tuple(r) for r in pdf[cols].itertuples(index=False, name=None)]
        execute_values(cur, f"""
            INSERT INTO silver_traffic ({', '.join(cols)}) VALUES %s
        """, rows)
    conn.commit()

    out_path = os.path.join(DATA_LAKE_PATH, 'silver', 'traffic')
    os.makedirs(out_path, exist_ok=True)
    pdf['date'] = pdf['recorded_at'].dt.strftime('%Y-%m-%d')
    pdf.to_parquet(out_path, partition_cols=['city', 'date'], engine='pyarrow', index=False)
    logging.info(f"✓ SILVER: {len(pdf)} baris bersih -> Postgres + Parquet")

# ============================================================
# GOLD — agregasi serving layer
# ============================================================
def compute_gold_comparison(conn, r, silver_df):
    pdf = (
        silver_df.groupBy('city', 'period')
        .agg(F.avg('congestion_index').alias('avg_congestion'),
             F.avg('current_speed').alias('avg_speed'),
             F.count('*').alias('record_count'))
        .toPandas()
    )
    if pdf.empty:
        return
    with conn.cursor() as cur:
        for _, row in pdf.iterrows():
            cur.execute("""
                INSERT INTO gold_city_comparison (city, period, avg_congestion, avg_speed, record_count)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (city, period) DO UPDATE SET
                    avg_congestion = EXCLUDED.avg_congestion,
                    avg_speed = EXCLUDED.avg_speed,
                    record_count = EXCLUDED.record_count,
                    computed_at = NOW()
            """, (row['city'], row['period'], float(row['avg_congestion']),
                  float(row['avg_speed']), int(row['record_count'])))
    conn.commit()

    # Bentuk ulang jadi struktur lama untuk kompatibilitas dashboard
    result = {}
    for city in pdf['city'].unique():
        cdf = pdf[pdf['city'] == city]
        b = cdf[cdf['period'] == 'before']
        a = cdf[cdf['period'] == 'after']
        before_ci = float(b['avg_congestion'].values[0]) if not b.empty else 0
        after_ci = float(a['avg_congestion'].values[0]) if not a.empty else 0
        result[city] = {
            'before': {'avg_congestion': round(before_ci, 2),
                       'avg_speed': round(float(b['avg_speed'].values[0]) if not b.empty else 0, 2),
                       'records': int(b['record_count'].values[0]) if not b.empty else 0},
            'after': {'avg_congestion': round(after_ci, 2),
                      'avg_speed': round(float(a['avg_speed'].values[0]) if not a.empty else 0, 2),
                      'records': int(a['record_count'].values[0]) if not a.empty else 0},
            'change_pct': round(after_ci - before_ci, 2),
        }
    r.set('comparison:all_cities', json.dumps(result))
    logging.info("✓ GOLD: city comparison")

def compute_gold_hourly(conn, r, silver_df):
    pdf = (
        silver_df.groupBy('city', 'period', 'hour')
        .agg(F.avg('congestion_index').alias('avg_congestion'),
             F.avg('current_speed').alias('avg_speed'))
        .toPandas()
    )
    if pdf.empty:
        return
    with conn.cursor() as cur:
        for _, row in pdf.iterrows():
            cur.execute("""
                INSERT INTO gold_hourly_pattern (city, hour, period, avg_congestion)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (city, hour, period) DO UPDATE SET
                    avg_congestion = EXCLUDED.avg_congestion, computed_at = NOW()
            """, (row['city'], int(row['hour']), row['period'], float(row['avg_congestion'])))
    conn.commit()

    for city in pdf['city'].unique():
        cdf = pdf[pdf['city'] == city]
        data = {}
        for _, row in cdf.iterrows():
            data.setdefault(row['period'], {})[int(row['hour'])] = {
                'avg_congestion': round(float(row['avg_congestion']), 2),
                'avg_speed': round(float(row['avg_speed']), 2),
            }
        r.set(f'hourly:{city}', json.dumps(data))
    logging.info("✓ GOLD: hourly pattern")

def compute_daily_summary(conn, silver_df):
    """Ringkasan harian + estimasi dampak kuantitatif (biaya BBM & waktu tempuh ekstra).
    Formula & asumsi didokumentasikan di README (bagian Metodologi Estimasi).
    Fallback ke tanggal terbaru yang tersedia jika belum ada data 'hari ini'
    (misal: live collector belum lama jalan) — supaya demo tetap representatif."""
    target_date_row = silver_df.select(F.max(F.date_format('recorded_at', 'yyyy-MM-dd')).alias('d')).collect()
    if not target_date_row or not target_date_row[0]['d']:
        return
    target_date = target_date_row[0]['d']

    pdf = (
        silver_df
        .withColumn('rec_date', F.date_format('recorded_at', 'yyyy-MM-dd'))
        .filter(F.col('rec_date') == target_date)
        .groupBy('city')
        .agg(F.avg('congestion_index').alias('avg_ci'),
             F.avg('current_speed').alias('avg_speed'),
             F.avg('free_flow_speed').alias('avg_ff_speed'))
        .toPandas()
    )
    if pdf.empty:
        return

    with conn.cursor() as cur:
        for _, row in pdf.iterrows():
            ci = float(row['avg_ci'])
            speed = float(row['avg_speed'])
            ff_speed = float(row['avg_ff_speed']) if row['avg_ff_speed'] else speed

            # Estimasi extra fuel cost: makin macet, makin tidak efisien BBM-nya
            penalty = 1 + (ci / 100.0) * MAX_CONGESTION_PENALTY
            liters_actual = (ASSUMED_DAILY_KM * penalty) / BASE_EFFICIENCY_KMPL
            liters_baseline = ASSUMED_DAILY_KM / BASE_EFFICIENCY_KMPL
            extra_liters = max(0, liters_actual - liters_baseline)
            extra_cost = round(extra_liters * FUEL_PRICE_AFTER_IDR, 2)

            # Estimasi extra travel time (menit) dibanding kondisi free-flow
            t_actual = (ASSUMED_DAILY_KM / speed) * 60 if speed > 0 else 0
            t_freeflow = (ASSUMED_DAILY_KM / ff_speed) * 60 if ff_speed > 0 else 0
            extra_min = round(max(0, t_actual - t_freeflow), 2)

            cur.execute("""
                INSERT INTO gold_daily_summary
                    (summary_date, city, avg_congestion_index, avg_speed,
                     est_extra_fuel_cost_idr, est_extra_travel_min)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (summary_date, city) DO UPDATE SET
                    avg_congestion_index = EXCLUDED.avg_congestion_index,
                    avg_speed = EXCLUDED.avg_speed,
                    est_extra_fuel_cost_idr = EXCLUDED.est_extra_fuel_cost_idr,
                    est_extra_travel_min = EXCLUDED.est_extra_travel_min,
                    computed_at = NOW()
            """, (target_date, row['city'], round(ci, 2), round(speed, 2), extra_cost, extra_min))
    conn.commit()
    logging.info(f"✓ GOLD: daily summary ({target_date}) + estimasi dampak")

def cache_bbm_prices(conn, r):
    df = pd.read_sql("SELECT * FROM bbm_prices ORDER BY effective_date DESC", conn)
    r.set('bbm:prices', df.to_json(orient='records'))

# ============================================================
# ML #1 — Forecasting (Spark MLlib RandomForest) + evaluasi
# ============================================================
def ml_forecast_rf(conn, r, silver_df):
    feature_cols = ['hour', 'day_of_week', 'is_weekend', 'current_speed', 'free_flow_speed']
    ml_df = silver_df.select(*feature_cols, F.col('congestion_level').alias('label'))

    total = ml_df.count()
    if total < 50:
        logging.info(f"Data kurang ({total} baris), skip ML forecasting")
        return None

    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    vec_df = assembler.transform(ml_df).select('features', 'label')
    train_df, test_df = vec_df.randomSplit([0.8, 0.2], seed=42)

    rf = RandomForestClassifier(featuresCol='features', labelCol='label', numTrees=100, seed=42)
    model = rf.fit(train_df)
    preds = model.transform(test_df)

    acc = MulticlassClassificationEvaluator(labelCol='label', metricName='accuracy').evaluate(preds)
    f1 = MulticlassClassificationEvaluator(labelCol='label', metricName='f1').evaluate(preds)
    train_n, test_n = train_df.count(), test_df.count()

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO gold_model_metrics (model_name, metric_name, metric_value, train_rows, test_rows)
            VALUES (%s,%s,%s,%s,%s)
        """, ('RandomForest_Congestion', 'accuracy', round(acc, 4), train_n, test_n))
        cur.execute("""
            INSERT INTO gold_model_metrics (model_name, metric_name, metric_value, train_rows, test_rows)
            VALUES (%s,%s,%s,%s,%s)
        """, ('RandomForest_Congestion', 'f1', round(f1, 4), train_n, test_n))
    conn.commit()
    r.set('ml:model_metrics', json.dumps({
        'model': 'RandomForestClassifier (Spark MLlib)',
        'accuracy': round(acc, 4), 'f1_score': round(f1, 4),
        'train_rows': train_n, 'test_rows': test_n,
        'evaluated_at': datetime.now().isoformat(),
    }))
    logging.info(f"✓ ML#1 RandomForest: accuracy={acc:.4f} f1={f1:.4f} (train={train_n} test={test_n})")

    # Prediksi 30 & 60 menit ke depan per kota
    now = datetime.now()
    predictions = {}
    avg_per_city = (
        silver_df.groupBy('city')
        .agg(F.avg('current_speed').alias('avg_speed'), F.avg('free_flow_speed').alias('avg_ff'))
        .toPandas()
    )
    rows_30, rows_60 = [], []
    for _, row in avg_per_city.iterrows():
        city = row['city']
        rows_30.append({'city': city, 'hour': (now.hour) % 24, 'day_of_week': now.isoweekday() % 7 + 1,
                         'is_weekend': int(now.weekday() >= 5),
                         'current_speed': float(row['avg_speed']), 'free_flow_speed': float(row['avg_ff'])})
        rows_60.append({'city': city, 'hour': (now.hour + 1) % 24, 'day_of_week': now.isoweekday() % 7 + 1,
                         'is_weekend': int(now.weekday() >= 5),
                         'current_speed': float(row['avg_speed']) * 0.95, 'free_flow_speed': float(row['avg_ff'])})

    # Build small spark dataframes for inference
    spark_session = silver_df.sparkSession
    pdf_30 = pd.DataFrame(rows_30)
    pdf_60 = pd.DataFrame(rows_60)
    sdf_30 = assembler.transform(spark_session.createDataFrame(pdf_30))
    sdf_60 = assembler.transform(spark_session.createDataFrame(pdf_60))
    pred_30 = model.transform(sdf_30).select('city', 'current_speed', F.col('prediction').alias('pred')).toPandas()
    pred_60 = model.transform(sdf_60).select('city', F.col('prediction').alias('pred')).toPandas()

    for _, row in pred_30.iterrows():
        city = row['city']
        p30 = int(row['pred'])
        p60 = int(pred_60[pred_60['city'] == city]['pred'].values[0])
        predictions[city] = {
            'current_avg_speed': round(float(row['current_speed']), 1),
            'predicted_30m': p30, 'predicted_30m_label': LEVEL_LABELS[p30],
            'predicted_60m': p60, 'predicted_60m_label': LEVEL_LABELS[p60],
            'updated_at': now.isoformat(),
        }
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gold_predictions (city, horizon_minutes, predicted_level, predicted_label, current_congestion)
                VALUES (%s,%s,%s,%s,%s)
            """, (city, 30, p30, LEVEL_LABELS[p30], None))
            cur.execute("""
                INSERT INTO gold_predictions (city, horizon_minutes, predicted_level, predicted_label, current_congestion)
                VALUES (%s,%s,%s,%s,%s)
            """, (city, 60, p60, LEVEL_LABELS[p60], None))
    conn.commit()
    r.set('ml:predictions', json.dumps(predictions))
    logging.info(f"✓ ML#1 prediksi 30/60 menit untuk {len(predictions)} kota")
    return model

# ============================================================
# ML #2 — Clustering spasial (KMeans): zona dampak kenaikan BBM
# ============================================================
def ml_cluster_zones(conn, r, silver_df):
    impact = (
        silver_df.groupBy('city', 'road_name')
        .agg(F.avg(F.when(F.col('period') == 'before', F.col('congestion_index'))).alias('ci_before'),
             F.avg(F.when(F.col('period') == 'after', F.col('congestion_index'))).alias('ci_after'),
             F.first('lat').alias('lat'), F.first('lon').alias('lon'))
        .withColumn('delta_congestion', F.col('ci_after') - F.col('ci_before'))
        .na.drop()
    )
    n = impact.count()
    if n < 3:
        logging.info(f"Ruas jalan dengan data before+after kurang ({n}), skip clustering")
        return

    k = min(3, n)
    assembler = VectorAssembler(inputCols=['ci_before', 'delta_congestion'], outputCol='features')
    feat_df = assembler.transform(impact)
    kmeans = KMeans(k=k, seed=42, featuresCol='features', predictionCol='zone_cluster')
    kmodel = kmeans.fit(feat_df)
    clustered = kmodel.transform(feat_df).toPandas()

    # Label cluster berdasarkan rata-rata delta_congestion: makin tinggi delta -> makin "Dampak Tinggi"
    cluster_rank = clustered.groupby('zone_cluster')['delta_congestion'].mean().sort_values()
    rank_to_label = {}
    labels_pool = ['Dampak Rendah', 'Dampak Sedang', 'Dampak Tinggi']
    for i, cluster_id in enumerate(cluster_rank.index):
        rank_to_label[cluster_id] = labels_pool[min(i, len(labels_pool) - 1)]
    clustered['zone_label'] = clustered['zone_cluster'].map(rank_to_label)

    with conn.cursor() as cur:
        for _, row in clustered.iterrows():
            cur.execute("""
                INSERT INTO gold_road_clusters
                    (city, road_name, lat, lon, ci_before, ci_after, delta_congestion, zone_cluster, zone_label)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (city, road_name) DO UPDATE SET
                    ci_before = EXCLUDED.ci_before, ci_after = EXCLUDED.ci_after,
                    delta_congestion = EXCLUDED.delta_congestion,
                    zone_cluster = EXCLUDED.zone_cluster, zone_label = EXCLUDED.zone_label,
                    computed_at = NOW()
            """, (row['city'], row['road_name'], float(row['lat']), float(row['lon']),
                  round(float(row['ci_before']), 2), round(float(row['ci_after']), 2),
                  round(float(row['delta_congestion']), 2), int(row['zone_cluster']), row['zone_label']))
    conn.commit()
    r.set('ml:clusters', clustered.to_json(orient='records'))
    logging.info(f"✓ ML#2 KMeans: {n} ruas jalan dikelompokkan jadi {k} zona dampak BBM")

# ============================================================
# ML #3 — Anomaly detection (Z-score): lonjakan kemacetan tak wajar
# ============================================================
def ml_anomaly_zscore(conn, r, silver_df):
    baseline = (
        silver_df.filter(F.col('period') == 'before')
        .groupBy('city', 'hour')
        .agg(F.avg('congestion_index').alias('mu'), F.stddev('congestion_index').alias('sigma'))
    )
    if baseline.count() == 0:
        logging.info("Belum ada data periode 'before' untuk baseline, skip anomaly detection")
        return

    after_df = silver_df.filter(F.col('period') == 'after')
    total_after = after_df.count()
    if total_after == 0:
        logging.info("Belum ada data periode 'after', skip anomaly detection")
        return

    scored = (
        after_df.join(baseline, on=['city', 'hour'], how='left')
        .withColumn('sigma_safe', F.when(F.col('sigma') > 0, F.col('sigma')).otherwise(F.lit(1.0)))
        .withColumn('zscore', (F.col('congestion_index') - F.col('mu')) / F.col('sigma_safe'))
        .withColumn('is_anomaly', (F.abs(F.col('zscore')) > 2.0).cast('int'))
    )
    anomalies = scored.filter(F.col('is_anomaly') == 1) \
        .select('city', 'road_name', 'recorded_at', 'congestion_index', 'mu', 'sigma', 'zscore') \
        .orderBy(F.abs(F.col('zscore')).desc()).limit(200).toPandas()
    anomaly_count = scored.filter(F.col('is_anomaly') == 1).count()
    rate_after = round(anomaly_count / total_after * 100, 2)

    # Rate periode 'before' juga dihitung sebagai pembanding (harus mendekati ~5% karena threshold z=2 ~ 95% CI)
    before_scored = (
        silver_df.filter(F.col('period') == 'before')
        .join(baseline, on=['city', 'hour'], how='left')
        .withColumn('sigma_safe', F.when(F.col('sigma') > 0, F.col('sigma')).otherwise(F.lit(1.0)))
        .withColumn('zscore', (F.col('congestion_index') - F.col('mu')) / F.col('sigma_safe'))
        .withColumn('is_anomaly', (F.abs(F.col('zscore')) > 2.0).cast('int'))
    )
    total_before = before_scored.count()
    anomaly_before = before_scored.filter(F.col('is_anomaly') == 1).count()
    rate_before = round(anomaly_before / total_before * 100, 2) if total_before else 0

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE gold_anomalies")
        if not anomalies.empty:
            rows = [tuple(r) for r in anomalies[['city', 'road_name', 'recorded_at', 'congestion_index',
                                                  'mu', 'sigma', 'zscore']].itertuples(index=False, name=None)]
            execute_values(cur, """
                INSERT INTO gold_anomalies
                    (city, road_name, recorded_at, congestion_index, baseline_mean, baseline_stddev, zscore)
                VALUES %s
            """, rows)
        for period, total, count, rate in [
            ('before', total_before, anomaly_before, rate_before),
            ('after', total_after, anomaly_count, rate_after),
        ]:
            cur.execute("""
                INSERT INTO gold_anomaly_rate (period, total_records, anomaly_count, anomaly_rate_pct)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (period) DO UPDATE SET
                    total_records = EXCLUDED.total_records, anomaly_count = EXCLUDED.anomaly_count,
                    anomaly_rate_pct = EXCLUDED.anomaly_rate_pct, computed_at = NOW()
            """, (period, total, count, rate))
    conn.commit()

    r.set('ml:anomaly_summary', json.dumps({
        'rate_before_pct': rate_before, 'rate_after_pct': rate_after,
        'total_before': total_before, 'total_after': total_after,
        'anomaly_count_after': anomaly_count,
    }))
    logging.info(f"✓ ML#3 Z-score anomaly: rate before={rate_before}% -> after={rate_after}% "
                 f"({anomaly_count}/{total_after} record terdeteksi anomali)")

# ============================================================
# MAIN
# ============================================================
def process(spark):
    logging.info("=== Mulai siklus Spark processing ===")
    try:
        conn = get_db()
        r = get_redis()
    except Exception as e:
        logging.error(f"Koneksi gagal: {e}")
        return

    try:
        bronze_df = load_bronze(conn, spark)
        if bronze_df is None:
            logging.info("Belum ada data Bronze, skip siklus ini")
            return

        silver_df = transform_silver(bronze_df).cache()
        write_silver(conn, silver_df)
        compute_gold_comparison(conn, r, silver_df)
        compute_gold_hourly(conn, r, silver_df)
        compute_daily_summary(conn, silver_df)
        cache_bbm_prices(conn, r)
        ml_forecast_rf(conn, r, silver_df)
        ml_cluster_zones(conn, r, silver_df)
        ml_anomaly_zscore(conn, r, silver_df)
        silver_df.unpersist()
    except Exception as e:
        logging.exception(f"Error saat processing: {e}")
    finally:
        conn.close()
    logging.info("=== Siklus selesai ===")

if __name__ == '__main__':
    logging.info("FuelWatch Spark Processor dimulai...")
    time.sleep(25)  # tunggu Postgres & data awal dari seeder/bronze_consumer

    spark = get_spark()
    spark.sparkContext.setLogLevel('WARN')

    def job():
        process(spark)

    job()
    schedule.every(5).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
