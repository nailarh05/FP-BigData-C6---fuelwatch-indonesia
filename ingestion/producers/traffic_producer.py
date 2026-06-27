"""
Traffic Producer
----------------
Polls TomTom Traffic API (or simulates data) and streams to `traffic-stream` Kafka topic.
TomTom Traffic API docs: https://developer.tomtom.com/traffic-api/documentation
"""

import json
import math
import os
import random
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from loguru import logger

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY", "")
TOPIC = "traffic-stream"
INTERVAL_SEC = 60

# Major road segments in each city (lat, lon, road_id)
ROAD_SEGMENTS: dict[str, list[dict]] = {
    "Jakarta": [
        {"road_id": "JKT_001", "name": "Tol Dalam Kota", "lat": -6.180, "lon": 106.820},
        {"road_id": "JKT_002", "name": "Jl. Sudirman", "lat": -6.208, "lon": 106.820},
        {"road_id": "JKT_003", "name": "Jl. HR Rasuna Said", "lat": -6.220, "lon": 106.835},
        {"road_id": "JKT_004", "name": "Tol JORR", "lat": -6.280, "lon": 106.790},
        {"road_id": "JKT_005", "name": "Jl. Gatot Subroto", "lat": -6.225, "lon": 106.808},
    ],
    "Surabaya": [
        {"road_id": "SBY_001", "name": "Jl. Ahmad Yani", "lat": -7.295, "lon": 112.738},
        {"road_id": "SBY_002", "name": "Tol Waru-Juanda", "lat": -7.370, "lon": 112.720},
        {"road_id": "SBY_003", "name": "Jl. Basuki Rahmat", "lat": -7.257, "lon": 112.744},
    ],
    "Bandung": [
        {"road_id": "BDG_001", "name": "Jl. Asia Afrika", "lat": -6.921, "lon": 107.609},
        {"road_id": "BDG_002", "name": "Tol Padalarang", "lat": -6.890, "lon": 107.530},
    ],
    "Medan": [
        {"road_id": "MDN_001", "name": "Jl. Diponegoro", "lat": 3.595, "lon": 98.672},
        {"road_id": "MDN_002", "name": "Jl. Gatot Subroto", "lat": 3.585, "lon": 98.680},
    ],
    "Makassar": [
        {"road_id": "MKS_001", "name": "Jl. Sultan Hasanuddin", "lat": -5.147, "lon": 119.432},
        {"road_id": "MKS_002", "name": "Jl. Perintis Kemerdekaan", "lat": -5.120, "lon": 119.478},
    ],
    "Semarang": [
        {"road_id": "SMG_001", "name": "Jl. Pandanaran", "lat": -6.990, "lon": 110.413},
        {"road_id": "SMG_002", "name": "Tol Semarang-Solo", "lat": -7.025, "lon": 110.450},
    ],
}


def _fetch_tomtom_traffic(lat: float, lon: float) -> dict | None:
    if not TOMTOM_API_KEY or TOMTOM_API_KEY == "demo_key":
        return None
    url = (
        f"https://api.tomtom.com/traffic/services/4/flowSegmentData/relative0/10/json"
        f"?point={lat},{lon}&key={TOMTOM_API_KEY}"
    )
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()["flowSegmentData"]
        return {
            "current_speed": data.get("currentSpeed", 0),
            "free_flow_speed": data.get("freeFlowSpeed", 50),
            "confidence": data.get("confidence", 1.0),
        }
    except Exception as exc:
        logger.warning(f"TomTom API error: {exc}")
        return None


def _hour_based_congestion(hour: int) -> float:
    """Rush hour pattern: 7–9 AM and 5–7 PM = high congestion."""
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        return random.uniform(60, 95)
    elif 10 <= hour <= 16:
        return random.uniform(30, 60)
    else:
        return random.uniform(10, 30)


def _simulate_traffic(road: dict, city: str) -> dict:
    hour = datetime.now().hour
    congestion_level = _hour_based_congestion(hour)
    free_flow = 60
    avg_speed = free_flow * (1 - congestion_level / 100) * random.uniform(0.9, 1.1)
    avg_speed = max(5, round(avg_speed, 1))

    mobility_index = round(100 - congestion_level + random.uniform(-5, 5), 2)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "city": city,
        "road_id": road["road_id"],
        "road_name": road["name"],
        "congestion_level": round(congestion_level, 2),
        "avg_speed": avg_speed,
        "free_flow_speed": free_flow,
        "mobility_index": max(0, min(100, mobility_index)),
        "source": "simulation" if not TOMTOM_API_KEY else "tomtom_api",
    }


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
    )
    logger.info(f"Traffic producer started → topic={TOPIC}")

    try:
        while True:
            events = []
            for city, roads in ROAD_SEGMENTS.items():
                for road in roads:
                    real_data = _fetch_tomtom_traffic(road["lat"], road["lon"])
                    if real_data:
                        event = {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "city": city,
                            "road_id": road["road_id"],
                            "road_name": road["name"],
                            "congestion_level": round(
                                (1 - real_data["current_speed"] / max(real_data["free_flow_speed"], 1)) * 100, 2
                            ),
                            "avg_speed": real_data["current_speed"],
                            "free_flow_speed": real_data["free_flow_speed"],
                            "mobility_index": round(real_data["confidence"] * 100, 2),
                            "source": "tomtom_api",
                        }
                    else:
                        event = _simulate_traffic(road, city)

                    events.append(event)
                    producer.send(TOPIC, key=road["road_id"], value=event)

            producer.flush()
            logger.info(f"Produced {len(events)} traffic events")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down traffic producer")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
