"""
FuelWatch Indonesia — Data Seeder (BATCH INGESTION ke BRONZE)
Generate data historis BEFORE & AFTER kenaikan BBM (1 Mei - 19 Juni 2026).
OpenWeatherMap dihapus — hanya traffic data yang digunakan di pipeline.
Fix: congestion minimum 5% agar tidak ada nilai 0% yang tidak realistis.
"""
import os, random
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH', '/data')

ROAD_POINTS = {
    'Jakarta': [
        {'name': 'Jl. Sudirman',       'lat': -6.2088,  'lon': 106.8175},
        {'name': 'Jl. Thamrin',        'lat': -6.1944,  'lon': 106.8229},
        {'name': 'Jl. HR Rasuna Said', 'lat': -6.2258,  'lon': 106.8317},
        {'name': 'Jl. Gatot Subroto',  'lat': -6.2335,  'lon': 106.8007},
        {'name': 'Jl. TB Simatupang',  'lat': -6.2897,  'lon': 106.7753},
    ],
    'Surabaya': [
        {'name': 'Jl. Ahmad Yani',     'lat': -7.3048,  'lon': 112.7373},
        {'name': 'Jl. Basuki Rahmat',  'lat': -7.2659,  'lon': 112.7469},
        {'name': 'Jl. Raya Darmo',     'lat': -7.2820,  'lon': 112.7313},
        {'name': 'Jl. Pemuda',         'lat': -7.2575,  'lon': 112.7521},
        {'name': 'Jl. MERR',           'lat': -7.2897,  'lon': 112.7897},
    ],
    'Yogyakarta': [
        {'name': 'Jl. Malioboro',      'lat': -7.7925,  'lon': 110.3663},
        {'name': 'Jl. Solo',           'lat': -7.7833,  'lon': 110.4166},
        {'name': 'Jl. Magelang',       'lat': -7.7614,  'lon': 110.3631},
        {'name': 'Ring Road Utara',    'lat': -7.7614,  'lon': 110.3897},
        {'name': 'Jl. Parangtritis',   'lat': -7.8319,  'lon': 110.3631},
    ],
}

CITY_BASE_CONGESTION = {
    'Jakarta':    {'base': 52, 'variance': 15},
    'Surabaya':   {'base': 38, 'variance': 12},
    'Yogyakarta': {'base': 28, 'variance': 10},
}
CITY_AFTER_CONGESTION = {
    'Jakarta':    {'base': 61, 'variance': 14},
    'Surabaya':   {'base': 45, 'variance': 12},
    'Yogyakarta': {'base': 33, 'variance': 10},
}
HOUR_MULTIPLIER = {
    0: 0.2, 1: 0.15, 2: 0.1, 3: 0.1, 4: 0.15, 5: 0.3,
    6: 0.7, 7: 1.4, 8: 1.8, 9: 1.5, 10: 1.1, 11: 1.0,
    12: 1.2, 13: 1.1, 14: 1.0, 15: 1.1, 16: 1.5,
    17: 1.9, 18: 1.8, 19: 1.4, 20: 1.0, 21: 0.7,
    22: 0.5, 23: 0.3,
}

def get_db():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB', 'fuelwatch'),
        user=os.getenv('POSTGRES_USER', 'fuelwatch'),
        password=os.getenv('POSTGRES_PASSWORD', 'fuelwatch123'),
        host=os.getenv('POSTGRES_HOST', 'postgres'),   # FIX: bukan localhost
        port=os.getenv('POSTGRES_PORT', '5432'),
    )

def generate_traffic_row(city, road, timestamp, period):
    hour = timestamp.hour
    is_wkend = timestamp.weekday() >= 5
    cfg = CITY_BASE_CONGESTION[city] if period == 'before' else CITY_AFTER_CONGESTION[city]
    multiplier = HOUR_MULTIPLIER[hour] * (0.6 if is_wkend else 1.0)

    congestion = cfg['base'] * multiplier + random.gauss(0, cfg['variance'])
    # FIX: minimum 5% agar tidak ada 0% yang tidak realistis
    congestion = max(5.0, min(100.0, congestion))

    free_flow = random.uniform(50, 70)
    current_spd = max(5.0, free_flow * (1 - congestion / 100) + random.gauss(0, 2))

    return {
        'city': city, 'road_name': road['name'],
        'current_speed': round(current_spd, 2), 'free_flow_speed': round(free_flow, 2),
        'congestion_index': round(congestion, 2),
        'lat': road['lat'], 'lon': road['lon'],
        'period': period, 'source': 'tomtom', 'ingested_via': 'batch_seed',
        'recorded_at': timestamp,
    }

def write_bronze_parquet(rows, subdir):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['recorded_at']).dt.strftime('%Y-%m-%d')
    out_path = os.path.join(DATA_LAKE_PATH, 'bronze', subdir)
    os.makedirs(out_path, exist_ok=True)
    df.to_parquet(out_path, partition_cols=['city', 'date'], engine='pyarrow', index=False)
    print(f"   ✓ Parquet bronze ditulis: {out_path} ({len(df):,} baris)")

def seed():
    print("=" * 55)
    print("  FuelWatch Indonesia - Data Seeder (Traffic Only)")
    print("=" * 55)
    try:
        conn = get_db()
        print("✓ Koneksi database berhasil")
    except Exception as e:
        print(f"✗ Koneksi gagal: {e}")
        return

    traffic_rows = []

    print("\n📅 Generate data BEFORE (1 Mei - 9 Juni 2026)...")
    ts = datetime(2026, 5, 1, 0, 0)
    while ts <= datetime(2026, 6, 9, 23, 30):
        for city, roads in ROAD_POINTS.items():
            for road in roads:
                traffic_rows.append(generate_traffic_row(city, road, ts, 'before'))
        ts += timedelta(minutes=30)
    print(f"   -> {len(traffic_rows):,} traffic rows siap")

    print("\n📅 Generate data AFTER (10 - 19 Juni 2026)...")
    ts = datetime(2026, 6, 10, 0, 0)
    while ts <= datetime(2026, 6, 19, 23, 30):
        for city, roads in ROAD_POINTS.items():
            for road in roads:
                traffic_rows.append(generate_traffic_row(city, road, ts, 'after'))
        ts += timedelta(minutes=30)
    print(f"   -> {len(traffic_rows):,} total traffic rows siap")

    print("\n💾 Menyimpan ke bronze_traffic (Postgres)...")
    with conn.cursor() as cur:
        rows = [(r['city'], r['road_name'], r['lat'], r['lon'], r['current_speed'],
                 r['free_flow_speed'], r['congestion_index'], r['period'],
                 r['source'], r['ingested_via'], r['recorded_at']) for r in traffic_rows]
        execute_values(cur, """
            INSERT INTO bronze_traffic
                (city, road_name, lat, lon, current_speed, free_flow_speed,
                 congestion_index, period, source, ingested_via, recorded_at)
            VALUES %s
        """, rows, page_size=2000)
        print(f"   ✓ {len(rows):,} traffic records -> bronze_traffic")
    conn.commit()
    conn.close()

    print("\n📦 Menulis salinan Parquet (lakehouse bronze)...")
    write_bronze_parquet(traffic_rows, 'traffic')

    print("\n" + "=" * 55)
    print("  ✅ SEEDING SELESAI!")
    print("  Bronze layer terisi. Tunggu spark_processor jalan")
    print("  (siklus pertama) untuk mengisi Silver & Gold layer.")
    print("  Buka dashboard: http://localhost:8501")
    print("=" * 55)

if __name__ == '__main__':
    seed()
