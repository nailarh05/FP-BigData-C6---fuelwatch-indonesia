"""
FuelWatch Bronze Consumer — STORAGE LAYER (Bronze)
Konsumsi message dari Kafka (hasil publish collector), lalu tulis ke:
  1. Postgres bronze_traffic / bronze_weather (raw, untuk query cepat)
  2. Parquet partitioned by city + date di /data/bronze/... (lakehouse, kolom-oriented,
     hemat storage & cepat untuk batch read oleh Spark)

Buffer di-flush setiap N pesan ATAU setiap T detik (mana yang lebih dulu),
supaya tidak menulis 1 file kecil per message (anti "small file problem" di lakehouse).
"""
import os
import json
import time
import logging
from datetime import datetime
from collections import deque

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [BRONZE] %(message)s')

KAFKA_BROKER   = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC_TRAFFIC  = 'fuelwatch.traffic.raw'
TOPIC_WEATHER  = 'fuelwatch.weather.raw'
DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH', '/data')
FLUSH_EVERY_SEC = 30
FLUSH_EVERY_N   = 50

def get_db():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'fuelwatch'),
        user=os.getenv('POSTGRES_USER', 'fuelwatch'),
        password=os.getenv('POSTGRES_PASSWORD', 'fuelwatch123'),
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
    )

def get_consumer(max_retries=30):
    for attempt in range(max_retries):
        try:
            return KafkaConsumer(
                TOPIC_TRAFFIC, TOPIC_WEATHER,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='fuelwatch-bronze-consumer',
                consumer_timeout_ms=5000,
            )
        except KafkaError as e:
            logging.warning(f"Kafka belum siap ({e}), retry {attempt+1}/{max_retries}...")
            time.sleep(5)
    raise RuntimeError("Tidak bisa konek ke Kafka setelah beberapa kali percobaan")

def write_parquet_partitioned(records, subdir):
    """Tulis batch records ke Parquet, dipartisi per city & date (medallion bronze)."""
    if not records:
        return
    df = pd.DataFrame(records)
    df['recorded_at'] = pd.to_datetime(df['recorded_at'])
    df['date'] = df['recorded_at'].dt.strftime('%Y-%m-%d')
    out_path = os.path.join(DATA_LAKE_PATH, 'bronze', subdir)
    os.makedirs(out_path, exist_ok=True)
    df.to_parquet(out_path, partition_cols=['city', 'date'], engine='pyarrow', index=False)
    logging.info(f"Parquet bronze ditulis: {out_path} ({len(df)} baris)")

def flush_traffic(conn, buf):
    if not buf:
        return
    rows = [(
        d['city'], d['road_name'], d['lat'], d['lon'], d['current_speed'],
        d['free_flow_speed'], d['congestion_index'], d['period'],
        d.get('source', 'tomtom'), d.get('ingested_via', 'kafka'), d['recorded_at']
    ) for d in buf]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO bronze_traffic
                (city, road_name, lat, lon, current_speed, free_flow_speed,
                 congestion_index, period, source, ingested_via, recorded_at)
            VALUES %s
        """, rows)
    conn.commit()
    write_parquet_partitioned(buf, 'traffic')
    logging.info(f"✓ {len(buf)} traffic record -> bronze_traffic (Postgres + Parquet)")

def flush_weather(conn, buf):
    if not buf:
        return
    rows = [(
        d['city'], d['temp'], d['humidity'], d['weather_desc'], d.get('rain_1h', 0),
        d.get('source', 'openweathermap'), d.get('ingested_via', 'kafka'), d['recorded_at']
    ) for d in buf]
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO bronze_weather
                (city, temp, humidity, weather_desc, rain_1h, source, ingested_via, recorded_at)
            VALUES %s
        """, rows)
    conn.commit()
    write_parquet_partitioned(buf, 'weather')
    logging.info(f"✓ {len(buf)} weather record -> bronze_weather (Postgres + Parquet)")

def main():
    logging.info("FuelWatch Bronze Consumer dimulai...")
    time.sleep(20)  # tunggu Kafka + Postgres siap

    conn = get_db()
    traffic_buf, weather_buf = deque(), deque()
    last_flush = time.time()

    while True:
        consumer = get_consumer()
        try:
            for msg in consumer:
                if msg.topic == TOPIC_TRAFFIC:
                    traffic_buf.append(msg.value)
                elif msg.topic == TOPIC_WEATHER:
                    weather_buf.append(msg.value)

                should_flush = (
                    len(traffic_buf) + len(weather_buf) >= FLUSH_EVERY_N
                    or (time.time() - last_flush) >= FLUSH_EVERY_SEC
                )
                if should_flush and (traffic_buf or weather_buf):
                    flush_traffic(conn, list(traffic_buf))
                    flush_weather(conn, list(weather_buf))
                    traffic_buf.clear()
                    weather_buf.clear()
                    last_flush = time.time()
        except Exception as e:
            logging.error(f"Consumer loop error: {e}")
        finally:
            # flush sisa buffer tiap kali consumer timeout (tidak ada message baru sejenak)
            if traffic_buf or weather_buf:
                try:
                    flush_traffic(conn, list(traffic_buf))
                    flush_weather(conn, list(weather_buf))
                except Exception as e:
                    logging.error(f"Flush error: {e}")
                traffic_buf.clear()
                weather_buf.clear()
                last_flush = time.time()
            consumer.close()
            time.sleep(2)

if __name__ == '__main__':
    main()
