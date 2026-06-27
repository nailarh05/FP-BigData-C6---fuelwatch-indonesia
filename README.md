# FuelWatch Indonesia

**Sistem Monitoring & Analitik Dampak Kenaikan Harga BBM terhadap Pola Mobilitas Masyarakat di Kota Besar Berbasis Big Data Real-Time**

Final Project — Mata Kuliah Big Data

---

## Daftar Anggota

| No | Nama Lengkap | NRP |
|----|--------------|-----|
| 1 | Hansen Chang | 5027241028 |
| 2 | Naila Raniyah Hanan | 5027241078 |
| 3 | Ahmad Ibnu Athaillah | 5027241024|


---

## Deskripsi Masalah

Kenaikan harga Bahan Bakar Minyak (BBM) di Indonesia berdampak langsung pada pola mobilitas masyarakat perkotaan. Namun, korelasi antara perubahan harga BBM dan pergerakan lalu lintas selama ini belum pernah dianalisis secara real-time dan sistematis. Proyek **FuelWatch** hadir untuk menjawab kesenjangan tersebut dengan membangun pipeline big data end-to-end yang mampu mengumpulkan, memproses, dan menganalisis data lalu lintas dan cuaca secara streaming, lalu menyajikannya dalam sebuah dashboard interaktif yang mudah dipahami.

---

## Tujuan Proyek

1. Membangun pipeline data streaming real-time menggunakan **Apache Kafka** untuk menangani data lalu lintas (TomTom API) dan cuaca (OpenWeatherMap API).
2. Mengimplementasikan arsitektur **Medallion Lakehouse** (Bronze → Silver → Gold) menggunakan **PostgreSQL** dan **Parquet** sebagai penyimpanan terdistribusi.
3. Melatih dan menjalankan **3 model Machine Learning** menggunakan Apache Spark MLlib: Random Forest (forecasting), K-Means (clustering), dan Z-score (anomaly detection).
4. Menyajikan seluruh wawasan dalam **dashboard interaktif Streamlit** dan **REST API FastAPI**.

---

## Dataset & Sumber Data

| Sumber | Jenis | Deskripsi |
|--------|-------|-----------|
| **TomTom Traffic API** | Real-time | Data kemacetan, kecepatan, dan kondisi jalan per ruas di kota besar |
| **Historical Seeder** | Batch | Data historis 1 Mei – 19 Juni 2026 (periode sebelum & sesudah kenaikan BBM) |

Tanggal acuan kenaikan BBM: **10 Juni 2026** (dapat dikonfigurasi melalui `.env`)

---

## Arsitektur Solusi (Medallion Lakehouse)

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

## Struktur Proyek

```
fuelwatch/
├── docker-compose.yml           # Unified — semua services
├── .env.example                 # Template konfigurasi
├── scripts/
│   └── start.sh                 # Script one-command startup
├── db/
│   └── init.sql                 # Medallion schema (Bronze/Silver/Gold)
├── collector/                   # Kafka Producer (TomTom + OpenWeather)
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── bronze_consumer/             # Kafka Consumer → Bronze layer
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark_processor/             # PySpark: Silver/Gold + 3 ML models
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── seeder/                      # Batch historical data → Bronze
│   ├── seed_data.py
│   └── Dockerfile
├── api/                         # FastAPI REST API
│   ├── main.py
│   ├── models.py
│   ├── requirements.txt
│   └── Dockerfile
├── dashboard/
│   └── frontend/                # Leaflet + Chart.js (dashboard sekunder)
├── ml/                          # Skrip ML offline
│   ├── kmeans_clustering.py
│   ├── lstm_mobility.py
│   └── correlation_analytics.py
├── processing/                  # Legacy Spark Streaming
│   └── spark_streaming/
│       ├── etl_pipeline.py
│       └── feature_engineering.py
├── ingestion/                   # Legacy producers
│   └── producers/
│       ├── fuel_price_producer.py
│       ├── traffic_producer.py
│       ├── weather_producer.py
│       └── transport_producer.py
└── data_lake/                   # Parquet lakehouse (bind-mount)
    ├── bronze/                  # Raw partitioned by city/date
    └── silver/                  # Cleaned, partitioned by city/date
```

---

## Tech Stack

| Kategori | Teknologi |
|----------|-----------|
| **Containerization** | Docker, Docker Compose |
| **Data Streaming** | Apache Kafka 3.7.0 (KRaft — tanpa Zookeeper) |
| **Data Lake Storage** | PostgreSQL 16, Apache Parquet |
| **Batch Ingestion** | Python Seeder (data historis) |
| **Stream Processing & ML** | Apache Spark (PySpark), MLlib |
| **Cache & Real-time** | Redis 7 |
| **API Service** | FastAPI, Uvicorn |
| **Dashboard Utama** | Streamlit |
| **Dashboard Sekunder** | HTML, JavaScript, Leaflet.js, Chart.js |
| **External APIs** | TomTom Traffic API, OpenWeatherMap API |

---

## Medallion Lakehouse

| Layer | Tabel | Isi |
|-------|-------|-----|
| **Bronze** | `bronze_traffic`, `bronze_weather` | Raw data dari Kafka + seeder (tanpa transformasi) |
| **Silver** | `silver_traffic` | Cleaned, dedup, feature engineering |
| **Gold** | `gold_city_comparison`, `gold_hourly_pattern`, `gold_daily_summary` | Agregat siap dashboard |
| **Gold ML** | `gold_predictions`, `gold_model_metrics`, `gold_road_clusters`, `gold_anomalies` | Hasil 3 model ML |

File Parquet tersimpan di `data_lake/bronze/` dan `data_lake/silver/` (partitioned by city/date).

---

##  Machine Learning (Spark MLlib)

| Model | Teknik | Output |
|-------|--------|--------|
| **Forecasting** | Random Forest Classifier | Prediksi congestion 30 & 60 menit ke depan |
| **Clustering** | K-Means | Zona dampak BBM per ruas jalan |
| **Anomaly Detection** | Z-score | Deteksi lonjakan kemacetan abnormal pasca kenaikan BBM |

Skrip ML offline tambahan tersedia di folder `ml/`:
- `lstm_mobility.py` — prediksi mobilitas dengan LSTM
- `correlation_analytics.py` — analisis korelasi BBM ↔ mobilitas

---

## Cara Menjalankan

### Prasyarat

- Docker & Docker Compose sudah ter-install dan Docker Engine sedang berjalan
- API Key dari [TomTom Developer](https://developer.tomtom.com/)

### 1. Setup Environment

```bash
cp .env.example .env
```

Buka file `.env` dan isi nilai berikut:

```env
TOMTOM_API_KEY=isi_api_key_tomtom_kamu_disini
```

### 2. Jalankan Proyek (1 Command)

```bash
chmod +x scripts/start.sh
./scripts/start.sh
```

Atau langsung dengan Docker Compose:

```bash
docker compose up --build -d
```

### 3. Tunggu Pipeline Pertama Selesai

Pipeline berjalan secara otomatis dalam urutan berikut:

| Tahap | Service | Estimasi Waktu | Keterangan |
|-------|---------|----------------|------------|
| 1 | `seeder` | ~1–2 menit | Mengisi Bronze dengan data historis (1 Mei – 19 Jun 2026) |
| 2 | `collector` | Berjalan terus | Mulai publish live data ke Kafka |
| 3 | `bronze_consumer` | Berjalan terus | Tulis data ke Postgres + Parquet |
| 4 | `spark_processor` | ~2–3 menit/siklus | Proses Silver/Gold/ML setiap 5 menit |

Monitor log spark processor:

```bash
docker compose logs -f spark_processor
```

### 4. Akses Dashboard & API

| Service | URL | Keterangan |
|---------|-----|------------|
| **Streamlit Dashboard** | http://localhost:8501 | Dashboard utama (5 tab analitik) |
| **FastAPI Swagger** | http://localhost:8000/docs | Dokumentasi REST API |
| **Leaflet Frontend** | http://localhost:3000 | Dashboard peta sekunder |

---

## API Endpoints (FastAPI)

```
GET  /health
GET  /api/v1/traffic/latest?city=Jakarta
GET  /api/v1/mobility/score?city=Jakarta
GET  /api/v1/forecast?city=Jakarta
GET  /api/v1/clusters
GET  /api/v1/alerts
GET  /api/v1/gold/summary          — snapshot Gold + Redis
GET  /api/v1/lakehouse/stats       — row count per layer
WS   /ws/live/{city}               — WebSocket real-time
```

---

## Port Mapping

Port disesuaikan agar tidak bentrok dengan service lokal (WSL-friendly):

| Service | Port Host | Catatan |
|---------|-----------|---------|
| PostgreSQL | **5433** | 5432 sering dipakai PostgreSQL lokal |
| Kafka | **9094** | 9092 sering dipakai Kafka/Hadoop lab |
| Redis | 6379 | |
| Streamlit | 8501 | |
| FastAPI | 8000 | |
| Frontend | 3000 | |

---

## Menghentikan Proyek

```bash
# Hentikan semua service
docker compose down

# Hentikan dan hapus semua data (termasuk volume)
docker compose down -v && rm -rf data_lake/bronze/* data_lake/silver/*
```

---

## Troubleshooting

| Masalah | Solusi |
|---------|--------|
| **Dashboard kosong** | Tunggu `spark_processor` selesai siklus pertama: `docker logs fuelwatch-spark-processor` |
| **Collector error API** | Pastikan `.env` berisi `TOMTOM_API_KEY` yang valid. Sistem tetap jalan dengan data seeder saja |
| **Port conflict** | Ubah port mapping di `docker-compose.yml` sesuai kebutuhan |
| **Seeder tidak jalan** | Pastikan PostgreSQL sudah healthy dulu sebelum seeder dimulai |

---

## Novelty

| Aspek | MyPertamina | Google Maps | **FuelWatch** |
|-------|:-----------:|:-----------:|:-------------:|
| Korelasi BBM ↔ mobilitas | ❌ | ❌ | ✅ |
| Medallion Lakehouse | ❌ | ❌ | ✅ |
| 3 teknik ML (RF + KMeans + Z-score) | ❌ | ❌ | ✅ |
| Estimasi dampak ekonomi (Rp/hari) | ❌ | ❌ | ✅ |
| Real-time Kafka streaming | ❌ | Parsial | ✅ |

---
