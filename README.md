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
├── docker-compose.yml
├── .env.example
├── scripts/start.sh
├── db/init.sql
├── collector/
├── bronze_consumer/
├── spark_processor/
├── seeder/
├── api/
├── dashboard/frontend/
├── ml/
├── processing/
├── ingestion/
└── data_lake/
    ├── bronze/
    └── silver/
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
| **External APIs** | TomTom Traffic API |

---

## Cara Menjalankan

### Prasyarat
- Docker & Docker Compose sudah ter-install
- API Key dari [TomTom Developer](https://developer.tomtom.com/)

### 1. Setup Environment

```bash
cp .env.example .env
# Edit .env → isi TOMTOM_API_KEY
```

### 2. Jalankan Proyek (1 Command)

```bash
docker compose up --build -d
```

### 3. Tunggu Pipeline Pertama Selesai

| Tahap | Service | Estimasi | Keterangan |
|-------|---------|----------|------------|
| 1 | `seeder` | ~1–2 menit | Isi Bronze dengan data historis |
| 2 | `collector` | Terus berjalan | Publish live data ke Kafka |
| 3 | `bronze_consumer` | Terus berjalan | Simpan ke Postgres + Parquet |
| 4 | `spark_processor` | ~2–3 menit/siklus | Proses Silver/Gold/ML tiap 5 menit |

Monitor log:
```bash
docker compose logs -f spark_processor
```

### 4. Akses Dashboard & API

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| FastAPI Swagger | http://localhost:8000/docs |
| Leaflet Frontend | http://localhost:3000 |

---

##  Screenshot

### 1. Docker — Semua Container Running
jalankan `docker compose ps` di terminal atau buka Docker Desktop, screenshot semua container berstatus **running/green**


<img width="1169" height="188" alt="image" src="https://github.com/user-attachments/assets/a4bf1e67-7a79-4d92-9773-b5914ce0d554" />

---

### 2. Kafka Streaming — Data Mengalir
terminal yang menampilkan log `collector` mengirim data dan `bronze_consumer` menerimanya
<img width="1205" height="195" alt="image" src="https://github.com/user-attachments/assets/c13ef334-54a4-40ee-a7be-e02d6e762c17" />



---

### 3. Streamlit Dashboard — Tab Overview
 buka http://localhost:8501 

<img width="665" height="636" alt="image" src="https://github.com/user-attachments/assets/882ab491-59f6-4eb1-9454-175b4913f5ed" />


---

### 4. Streamlit Dashboard — Tab Analitik
 klik tab-tab lain di http://localhost:8501 

<img width="675" height="614" alt="image" src="https://github.com/user-attachments/assets/4ec2abdf-8dc7-4fed-be88-4044d3d02cde" />


---

### 5. FastAPI Swagger
 buka http://localhost:8000/docs 
<img width="653" height="619" alt="image" src="https://github.com/user-attachments/assets/c0f7e382-7d95-4461-8218-68ca5a04e9c3" />


---

### 6. Leaflet Frontend — Peta Interaktif
 buka http://localhost:3000
<img width="687" height="523" alt="image" src="https://github.com/user-attachments/assets/ba130a55-3adc-4229-aa06-ac24a1e90d40" />


---

### 7. Spark Processor — ML Berjalan
 jalankan `docker compose logs spark_processor`
<img width="1171" height="197" alt="image" src="https://github.com/user-attachments/assets/a11ff6dc-644b-4a39-80b5-4166db99d9e6" />


---

## 🥉🥈🥇 Medallion Lakehouse

| Layer | Tabel | Isi |
|-------|-------|-----|
| **Bronze** | `bronze_traffic`, `bronze_weather` | Raw data dari Kafka + seeder |
| **Silver** | `silver_traffic` | Cleaned, dedup, feature engineering |
| **Gold** | `gold_city_comparison`, `gold_hourly_pattern`, `gold_daily_summary` | Agregat siap dashboard |
| **Gold ML** | `gold_predictions`, `gold_model_metrics`, `gold_road_clusters`, `gold_anomalies` | Hasil 3 model ML |

---

## 🤖Machine Learning (Spark MLlib)

| Model | Teknik | Output |
|-------|--------|--------|
| **Forecasting** | Random Forest Classifier | Prediksi congestion 30 & 60 menit ke depan |
| **Clustering** | K-Means | Zona dampak BBM per ruas jalan |
| **Anomaly Detection** | Z-score | Deteksi lonjakan kemacetan abnormal pasca kenaikan BBM |

---

## 🔌 API Endpoints (FastAPI)

```
GET  /health
GET  /api/v1/traffic/latest?city=Jakarta
GET  /api/v1/mobility/score?city=Jakarta
GET  /api/v1/forecast?city=Jakarta
GET  /api/v1/clusters
GET  /api/v1/alerts
GET  /api/v1/gold/summary
GET  /api/v1/lakehouse/stats
WS   /ws/live/{city}
```

---

## Port Mapping

| Service | Port |
|---------|------|
| PostgreSQL | 5433 |
| Kafka | 9094 |
| Redis | 6379 |
| Streamlit | 8501 |
| FastAPI | 8000 |
| Frontend | 3000 |

---

## Menghentikan Proyek

```bash
docker compose down

# Hapus semua data juga
docker compose down -v && rm -rf data_lake/bronze/* data_lake/silver/*
```

---

## 🔧 Troubleshooting

| Masalah | Solusi |
|---------|--------|
| **Dashboard kosong** | Tunggu `spark_processor` siklus pertama selesai |
| **Collector error API** | Pastikan `TOMTOM_API_KEY` di `.env` valid |
| **Port conflict** | Ubah port di `docker-compose.yml` |

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

*Dikembangkan untuk Final Project Mata Kuliah Big Data*
