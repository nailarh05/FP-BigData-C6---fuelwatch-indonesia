"""
FuelWatch Collector — INGESTION LAYER
Mengambil data real-time dari TomTom Traffic API,
lalu mem-publish ke Kafka topics.
(OpenWeatherMap dihapus — cuaca tidak digunakan di pipeline analitik)
"""
import os, time, json, logging, requests, schedule, redis
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s [COLLECTOR] %(message)s')

TOMTOM_KEY    = os.getenv('TOMTOM_API_KEY')
BBM_DATE      = os.getenv('BBM_EVENT_DATE', '2026-06-10')
KAFKA_BROKER  = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC_TRAFFIC = 'fuelwatch.traffic.raw'

ROAD_POINTS = {
    'Jakarta': [
        {'name': 'Jl. Sudirman',        'lat': -6.2088,  'lon': 106.8175},
        {'name': 'Jl. Thamrin',         'lat': -6.1944,  'lon': 106.8229},
        {'name': 'Jl. HR Rasuna Said',  'lat': -6.2258,  'lon': 106.8317},
        {'name': 'Jl. Gatot Subroto',   'lat': -6.2335,  'lon': 106.8007},
        {'name': 'Jl. TB Simatupang',   'lat': -6.2897,  'lon': 106.7753},
    ],
    'Surabaya': [
        {'name': 'Jl. Ahmad Yani',      'lat': -7.3048,  'lon': 112.7373},
        {'name': 'Jl. Basuki Rahmat',   'lat': -7.2659,  'lon': 112.7469},
        {'name': 'Jl. Raya Darmo',      'lat': -7.2820,  'lon': 112.7313},
        {'name': 'Jl. Pemuda',          'lat': -7.2575,  'lon': 112.7521},
        {'name': 'Jl. MERR',            'lat': -7.2897,  'lon': 112.7897},
    ],
    'Yogyakarta': [
        {'name': 'Jl. Malioboro',       'lat': -7.7925,  'lon': 110.3663},
        {'name': 'Jl. Solo',            'lat': -7.7833,  'lon': 110.4166},
        {'name': 'Jl. Magelang',        'lat': -7.7614,  'lon': 110.3631},
        {'name': 'Ring Road Utara',     'lat': -7.7614,  'lon': 110.3897},
        {'name': 'Jl. Parangtritis',    'lat': -7.8319,  'lon': 110.3631},
    ],
}

def get_redis():
    return redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True
    )

def get_producer(max_retries=30):
    for attempt in range(max_retries):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5, linger_ms=200,
            )
        except KafkaError as e:
            logging.warning(f"Kafka belum siap ({e}), retry {attempt+1}/{max_retries}...")
            time.sleep(5)
    raise RuntimeError("Tidak bisa konek ke Kafka")

def get_period():
    return 'after' if datetime.now().date() >= datetime.strptime(BBM_DATE, '%Y-%m-%d').date() else 'before'

def fetch_traffic(city, point):
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
        f"?point={point['lat']},{point['lon']}&key={TOMTOM_KEY}"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        d = r.json().get('flowSegmentData', {})
        current_speed   = d.get('currentSpeed', 0)
        free_flow_speed = d.get('freeFlowSpeed', 1)
        # Minimum congestion 5% agar tidak ada 0% yang tidak realistis
        congestion_idx  = max(5.0, (free_flow_speed - current_speed) / free_flow_speed * 100)
        return {
            'city': city, 'road_name': point['name'],
            'current_speed': current_speed, 'free_flow_speed': free_flow_speed,
            'congestion_index': round(congestion_idx, 2),
            'lat': point['lat'], 'lon': point['lon'],
            'period': get_period(), 'source': 'tomtom',
            'ingested_via': 'kafka', 'recorded_at': datetime.now().isoformat(),
        }
    except Exception as e:
        logging.error(f"Traffic error {city} - {point['name']}: {e}")
        return None

def cache_latest(r, data_list):
    grouped = {}
    for d in data_list:
        grouped.setdefault(d['city'], []).append(d)
    for city, items in grouped.items():
        avg_idx = sum(i['congestion_index'] for i in items) / len(items)
        avg_spd = sum(i['current_speed'] for i in items) / len(items)
        r.setex(f"latest:{city}", 120, json.dumps({
            'city': city,
            'avg_congestion_index': round(avg_idx, 2),
            'avg_speed': round(avg_spd, 2),
            'period': items[0]['period'],
            'updated_at': datetime.now().isoformat(),
            'roads': items
        }))

def collect(producer, r):
    logging.info("=== Mulai koleksi data TomTom ===")
    all_traffic = []
    for city, points in ROAD_POINTS.items():
        for point in points:
            data = fetch_traffic(city, point)
            if data:
                producer.send(TOPIC_TRAFFIC, value=data)
                all_traffic.append(data)
                logging.info(f"-> {city} - {point['name']}: {data['congestion_index']:.1f}%")
            time.sleep(0.5)
    producer.flush()
    if all_traffic:
        cache_latest(r, all_traffic)
    logging.info(f"=== Selesai: {len(all_traffic)} titik diproses ===")

if __name__ == '__main__':
    logging.info("FuelWatch Collector dimulai (TomTom only, no OpenWeather)...")
    time.sleep(15)
    producer = get_producer()
    r = get_redis()
    collect(producer, r)
    schedule.every(60).seconds.do(lambda: collect(producer, r))
    while True:
        schedule.run_pending()
        time.sleep(1)
