# FuelWatch Indonesia

**Sistem Monitoring & Analitik Dampak Kenaikan Harga BBM terhadap Pola Mobilitas Masyarakat di Kota Besar Berbasis Big Data Real-Time**

Final Project — Mata Kuliah Big Data

> **Integrasi v2:** Pipeline `fuelwatch_v2` (Medallion Lakehouse + Spark MLlib + Streamlit) telah diintegrasikan sebagai **pipeline utama**. Komponen v1 (FastAPI REST, Leaflet dashboard, skrip ML offline) tetap tersedia sebagai lapisan tambahan.

---

## Arsitektur Terintegrasi

```
TomTom API + OpenWeatherMap
        │
        ▼
  collector ──► Kafka (fuelwatch.traffic.raw, fuelwatch.weather.raw)
        │              │
        │              ▼
        │     bronze_consumer ──► BRONZE (Postgres + Parquet)
        │              │
        ▼              ▼
  Redis (cache)   spark_processor (PySpark, siklus 5 menit)
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
           SILVER      GOLD      ML (RF + KMeans + Z-score)
              │          │          │
              └──────────┴──────────┘
                         │
         ┌───────────────┼───────────────┐
         ▼               ▼               ▼
  Streamlit :8501   FastAPI :8000   Leaflet :3000
  (dashboard utama) (REST API)      (dashboard sekunder)
```

---

## Struktur Project

```
fpbigdat/
├── docker-compose.yml           # Unified — semua services v2 + v1
├── .env.example
├── db/init.sql                  # Medallion schema (Bronze/Silver/Gold)
├── collector/                   # Kafka Producer (TomTom + OpenWeather)
├── bronze_consumer/             # Kafka Consumer → Bronze layer
├── spark_processor/             # PySpark: Silver/Gold + 3 ML models
├── seeder/                      # Batch historical data → Bronze
├── dashboard/
│   ├── streamlit/               # Dashboard utama (v2)
│   └── frontend/                # Leaflet + Chart.js (v1)
├── api/                         # FastAPI — baca Gold layer + Redis
├── ml/                          # Skrip ML offline (LSTM, korelasi)
├── ingestion/                   # Legacy producers v1 (opsional)
├── processing/                  # Legacy Spark Streaming v1 (opsional)
├── data_lake/                   # Parquet lakehouse (bind-mount)
└── fuelwatch_v2/                # Salinan asli (legacy reference)
```

---

## Cara Menjalankan

### 1. Setup environment

```bash
cp .env.example .env
# Edit .env — isi TOMTOM_API_KEY dan OPENWEATHER_API_KEY
```

### 2. Start semua services

```bash
chmod +x scripts/start.sh
./scripts/start.sh
```

Atau manual:

```bash
docker compose up --build -d
```

### 3. Akses dashboard & API

| Service | URL | Keterangan |
|---------|-----|------------|
| **Streamlit Dashboard** | http://localhost:8501 | Dashboard utama v2 (5 tab) |
| FastAPI Swagger | http://localhost:8000/docs | REST API Gold layer |
| Leaflet Frontend | http://localhost:3000 | Dashboard sekunder v1 |
| PostgreSQL | localhost:5433 | user: `fuelwatch` / pass: `fuelwatch123` |
| Kafka | localhost:9094 | Apache KRaft (tanpa Zookeeper) |

### 4. Tunggu pipeline pertama

1. `seeder` — isi Bronze historis (1 Mei–19 Jun 2026) → selesai ~1-2 menit
2. `collector` — mulai publish live data ke Kafka
3. `bronze_consumer` — tulis ke Postgres + Parquet
4. `spark_processor` — siklus pertama Silver/Gold/ML → ~2-3 menit

```bash
docker compose logs -f spark_processor
```

---

## Medallion Lakehouse (fuelwatch_v2)

| Layer | Tabel | Isi |
|-------|-------|-----|
| **Bronze** | `bronze_traffic`, `bronze_weather` | Raw data dari Kafka + seeder |
| **Silver** | `silver_traffic` | Cleaned, dedup, feature engineering |
| **Gold** | `gold_city_comparison`, `gold_hourly_pattern`, `gold_daily_summary` | Agregat siap dashboard |
| **Gold ML** | `gold_predictions`, `gold_model_metrics`, `gold_road_clusters`, `gold_anomalies` | Hasil 3 model ML |

Parquet files: `data_lake/bronze/` dan `data_lake/silver/` (partitioned by city/date)

---

## Machine Learning (Spark MLlib)

| Model | Teknik | Output |
|-------|--------|--------|
| Forecasting | Random Forest Classifier | Prediksi congestion 30 & 60 menit |
| Clustering | K-Means | Zona dampak BBM per ruas jalan |
| Anomaly Detection | Z-score | Lonjakan kemacetan abnormal pasca BBM |

Skrip ML offline tambahan (v1): `ml/lstm_mobility.py`, `ml/correlation_analytics.py`

---

## API Endpoints (FastAPI — terintegrasi Gold layer)

```
GET  /health
GET  /api/v1/traffic/latest?city=Jakarta
GET  /api/v1/mobility/score?city=Jakarta
GET  /api/v1/forecast?city=Jakarta
GET  /api/v1/clusters
GET  /api/v1/alerts
GET  /api/v1/gold/summary          ← baru: snapshot Gold + Redis
GET  /api/v1/lakehouse/stats       ← baru: row count per layer
WS   /ws/live/{city}
```

---

## Port Mapping (WSL-friendly)

Port disesuaikan agar tidak bentrok dengan service lokal:

| Service | Port Host | Catatan |
|---------|-----------|---------|
| PostgreSQL | **5433** | 5432 sering dipakai PostgreSQL lokal |
| Kafka | **9094** | 9092 sering dipakai Kafka/Hadoop lab |
| Redis | 6379 | |
| Streamlit | 8501 | |
| FastAPI | 8000 | |
| Frontend | 3000 | |

---

## Troubleshooting

- **Dashboard kosong** → tunggu `spark_processor` selesai siklus pertama: `docker logs fuelwatch-spark-processor`
- **Collector error API** → pastikan `.env` berisi API key valid; sistem tetap jalan dengan data seeder
- **Reset semua data** → `docker compose down -v && rm -rf data_lake/bronze/* data_lake/silver/*`
- **Port conflict** → ubah mapping di `docker-compose.yml`

---

## Novelty

| Aspek | MyPertamina | Google Maps | **FuelWatch** |
|-------|-------------|-------------|---------------|
| Korelasi BBM ↔ mobilitas | ❌ | ❌ | ✅ |
| Medallion lakehouse | ❌ | ❌ | ✅ |
| 3 teknik ML (RF + KMeans + Z-score) | ❌ | ❌ | ✅ |
| Estimasi dampak ekonomi (Rp/hari) | ❌ | ❌ | ✅ |
| Real-time Kafka streaming | ❌ | Parsial | ✅ |

---

*Dikembangkan untuk Final Project Mata Kuliah Big Data*
