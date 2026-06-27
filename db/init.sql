-- ============================================================
-- FuelWatch Indonesia — Medallion Lakehouse Schema
-- Bronze  = raw ingested data (apa adanya dari sumber)
-- Silver  = cleaned, deduplicated, feature-engineered
-- Gold    = aggregated, model-ready, dipakai langsung oleh dashboard
-- ============================================================

-- ---------- DIMENSION / REFERENCE ----------
CREATE TABLE IF NOT EXISTS cities (
    city_id     SERIAL PRIMARY KEY,
    city_name   VARCHAR(50) UNIQUE NOT NULL,
    lat         DECIMAL(9,6),
    lon         DECIMAL(9,6)
);

INSERT INTO cities (city_name, lat, lon) VALUES
    ('Jakarta', -6.200000, 106.816666),
    ('Surabaya', -7.250445, 112.768845),
    ('Yogyakarta', -7.797068, 110.370529)
ON CONFLICT (city_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS bbm_prices (
    id          SERIAL PRIMARY KEY,
    fuel_type   VARCHAR(50) NOT NULL,
    price_before DECIMAL(10,2) NOT NULL,
    price_after  DECIMAL(10,2) NOT NULL,
    effective_date DATE NOT NULL,
    source_url  TEXT
);

INSERT INTO bbm_prices (fuel_type, price_before, price_after, effective_date, source_url) VALUES
    ('Pertamax', 12300, 16250, '2026-06-10', 'https://www.cnbcindonesia.com/news/20260610000254-4-741535'),
    ('Pertamax Green 95', 12900, 17000, '2026-06-10', 'https://www.cnbcindonesia.com/news/20260610000254-4-741535')
ON CONFLICT DO NOTHING;

-- ============================================================
-- BRONZE LAYER — raw, append-only, minimal transformation
-- Diisi oleh: bronze_consumer (dari Kafka, real-time) + seeder (historical batch load)
-- ============================================================
CREATE TABLE IF NOT EXISTS bronze_traffic (
    id                SERIAL PRIMARY KEY,
    city              VARCHAR(50) NOT NULL,
    road_name         VARCHAR(100) NOT NULL,
    lat               DECIMAL(9,6),
    lon               DECIMAL(9,6),
    current_speed     DECIMAL(6,2),
    free_flow_speed   DECIMAL(6,2),
    congestion_index  DECIMAL(6,2),
    period            VARCHAR(10),           -- 'before' / 'after' relatif tgl BBM naik
    source             VARCHAR(20) DEFAULT 'tomtom',
    ingested_via       VARCHAR(20) DEFAULT 'kafka',  -- 'kafka' (real-time) atau 'batch_seed' (historical)
    recorded_at       TIMESTAMP NOT NULL,
    ingested_at       TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bronze_traffic_city_time ON bronze_traffic (city, recorded_at);

CREATE TABLE IF NOT EXISTS bronze_weather (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50) NOT NULL,
    temp            DECIMAL(5,2),
    humidity        INTEGER,
    weather_desc    VARCHAR(100),
    rain_1h         DECIMAL(6,2),
    source          VARCHAR(20) DEFAULT 'openweathermap',
    ingested_via    VARCHAR(20) DEFAULT 'kafka',
    recorded_at     TIMESTAMP NOT NULL,
    ingested_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bronze_weather_city_time ON bronze_weather (city, recorded_at);

-- ============================================================
-- SILVER LAYER — cleaned + feature engineered (ditulis ulang oleh spark_processor)
-- Transformasi: dedup, filter nilai invalid, tambah fitur waktu, label congestion_level
-- ============================================================
CREATE TABLE IF NOT EXISTS silver_traffic (
    id                SERIAL PRIMARY KEY,
    city              VARCHAR(50) NOT NULL,
    road_name         VARCHAR(100) NOT NULL,
    lat               DECIMAL(9,6),
    lon               DECIMAL(9,6),
    current_speed     DECIMAL(6,2),
    free_flow_speed   DECIMAL(6,2),
    congestion_index  DECIMAL(6,2),
    congestion_level  SMALLINT,         -- 0=lancar 1=ramai-lancar 2=padat 3=macet
    period            VARCHAR(10),
    hour              SMALLINT,
    day_of_week       SMALLINT,
    is_weekend        SMALLINT,
    recorded_at       TIMESTAMP NOT NULL,
    processed_at      TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_silver_traffic_city_time ON silver_traffic (city, recorded_at);

-- ============================================================
-- GOLD LAYER — agregat siap-saji untuk dashboard & serving
-- Ditulis oleh spark_processor setiap siklus batch (default 5 menit)
-- ============================================================

-- Perbandingan rata-rata sebelum vs sesudah kenaikan BBM, per kota
CREATE TABLE IF NOT EXISTS gold_city_comparison (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50) NOT NULL,
    period          VARCHAR(10) NOT NULL,
    avg_congestion  DECIMAL(6,2),
    avg_speed       DECIMAL(6,2),
    record_count    INTEGER,
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, period)
);

-- Pola kemacetan per jam (untuk heatmap dashboard)
CREATE TABLE IF NOT EXISTS gold_hourly_pattern (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50) NOT NULL,
    hour            SMALLINT NOT NULL,
    period          VARCHAR(10) NOT NULL,
    avg_congestion  DECIMAL(6,2),
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, hour, period)
);

-- Ringkasan harian + estimasi dampak (rupiah & waktu tempuh)
CREATE TABLE IF NOT EXISTS gold_daily_summary (
    id                      SERIAL PRIMARY KEY,
    summary_date            DATE NOT NULL,
    city                    VARCHAR(50) NOT NULL,
    avg_congestion_index    DECIMAL(6,2),
    avg_speed               DECIMAL(6,2),
    est_extra_fuel_cost_idr DECIMAL(12,2),   -- estimasi biaya BBM tambahan akibat macet
    est_extra_travel_min    DECIMAL(6,2),    -- estimasi tambahan waktu tempuh per perjalanan
    computed_at             TIMESTAMP DEFAULT NOW(),
    UNIQUE(summary_date, city)
);

-- ML #1: prediksi forecasting (RandomForest, horizon 30 & 60 menit)
CREATE TABLE IF NOT EXISTS gold_predictions (
    id                  SERIAL PRIMARY KEY,
    city                VARCHAR(50) NOT NULL,
    horizon_minutes     SMALLINT NOT NULL,
    predicted_level     SMALLINT,
    predicted_label     VARCHAR(20),
    current_congestion  DECIMAL(6,2),
    change_pct          DECIMAL(6,2),
    predicted_at        TIMESTAMP DEFAULT NOW()
);

-- ML #1 evaluasi model (akurasi, F1 — CPMK-4 requirement)
CREATE TABLE IF NOT EXISTS gold_model_metrics (
    id              SERIAL PRIMARY KEY,
    model_name      VARCHAR(50) NOT NULL,
    metric_name     VARCHAR(20) NOT NULL,     -- 'accuracy' / 'f1'
    metric_value    DECIMAL(6,4),
    train_rows      INTEGER,
    test_rows       INTEGER,
    evaluated_at    TIMESTAMP DEFAULT NOW()
);

-- ML #2: clustering spasial — zona dampak kenaikan BBM (KMeans)
CREATE TABLE IF NOT EXISTS gold_road_clusters (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50) NOT NULL,
    road_name       VARCHAR(100) NOT NULL,
    lat             DECIMAL(9,6),
    lon             DECIMAL(9,6),
    ci_before       DECIMAL(6,2),
    ci_after        DECIMAL(6,2),
    delta_congestion DECIMAL(6,2),
    zone_cluster    SMALLINT,            -- 0/1/2 = label cluster mentah dari KMeans
    zone_label      VARCHAR(30),         -- 'Dampak Tinggi' / 'Dampak Sedang' / 'Dampak Rendah'
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, road_name)
);

-- ML #3: anomaly detection (Z-score) — lonjakan kemacetan tak wajar setelah BBM naik
CREATE TABLE IF NOT EXISTS gold_anomalies (
    id              SERIAL PRIMARY KEY,
    city            VARCHAR(50) NOT NULL,
    road_name       VARCHAR(100) NOT NULL,
    recorded_at     TIMESTAMP NOT NULL,
    congestion_index DECIMAL(6,2),
    baseline_mean   DECIMAL(6,2),
    baseline_stddev DECIMAL(6,2),
    zscore          DECIMAL(6,3),
    detected_at     TIMESTAMP DEFAULT NOW()
);

-- Ringkasan tingkat anomali sebelum vs sesudah (angka kuantitatif untuk CPMK-4)
CREATE TABLE IF NOT EXISTS gold_anomaly_rate (
    id              SERIAL PRIMARY KEY,
    period          VARCHAR(10) NOT NULL UNIQUE,
    total_records   INTEGER,
    anomaly_count   INTEGER,
    anomaly_rate_pct DECIMAL(6,2),
    computed_at     TIMESTAMP DEFAULT NOW()
);

-- View cepat untuk dashboard "kondisi terbaru"
CREATE OR REPLACE VIEW v_latest_traffic AS
SELECT DISTINCT ON (city, road_name)
    city, road_name, lat, lon, current_speed, free_flow_speed,
    congestion_index, period, recorded_at
FROM bronze_traffic
ORDER BY city, road_name, recorded_at DESC;
