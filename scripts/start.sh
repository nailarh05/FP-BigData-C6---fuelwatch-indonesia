#!/usr/bin/env bash
# FuelWatch Indonesia — Unified Start (v1 + fuelwatch_v2)
set -euo pipefail

if docker compose version &>/dev/null; then
  DC="docker compose"
elif command -v docker-compose &>/dev/null; then
  DC="docker-compose"
else
  echo "[ERROR] Docker Compose not found."
  exit 1
fi

echo "=================================================="
echo "  FuelWatch Indonesia — Unified Big Data System"
echo "  Pipeline: fuelwatch_v2 (Medallion + Spark ML)"
echo "  Using: $DC"
echo "=================================================="

if [ ! -f .env ]; then
  cp .env.example .env
  echo "[INFO] .env created — isi TOMTOM_API_KEY & OPENWEATHER_API_KEY"
fi

mkdir -p data_lake/bronze data_lake/silver

echo "[1/2] Building & starting all services..."
$DC up --build -d

echo ""
echo "[2/2] Menunggu pipeline pertama (seeder → spark)..."
echo "      Seeder: ~1-2 menit | Spark siklus pertama: ~2-3 menit"
echo ""
echo "Services:"
echo "  Streamlit Dashboard (PRIMARY) → http://localhost:8501"
echo "  FastAPI Swagger              → http://localhost:8000/docs"
echo "  Leaflet Frontend (secondary) → http://localhost:3000"
echo "  PostgreSQL                   → localhost:5433"
echo "  Kafka                        → localhost:9094"
echo ""
echo "Logs:  $DC logs -f spark_processor"
echo "Stop:  $DC down"
