"""
Public Transport Producer
--------------------------
Streams ridership / volume data for buses, KRL, MRT, LRT, and angkot
to the `transport-stream` Kafka topic.

Data Sources (production):
- BPS (Badan Pusat Statistik) — monthly ridership datasets
- Kemenhub Open Data API
- TransJakarta Open API
- KAI Commuter API

For now: simulated data with realistic patterns.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from loguru import logger

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transport-stream"
INTERVAL_SEC = 120

TRANSPORT_SYSTEMS: dict[str, list[dict]] = {
    "Jakarta": [
        {"mode": "TransJakarta BRT", "code": "TJ", "capacity": 1200, "routes": 13},
        {"mode": "KRL Commuter Line", "code": "KRL", "capacity": 1800, "routes": 8},
        {"mode": "MRT Jakarta", "code": "MRT", "capacity": 1000, "routes": 2},
        {"mode": "LRT Jakarta", "code": "LRT", "capacity": 600, "routes": 1},
        {"mode": "Angkot", "code": "ANG", "capacity": 300, "routes": 45},
    ],
    "Surabaya": [
        {"mode": "Suroboyo Bus", "code": "SB", "capacity": 500, "routes": 5},
        {"mode": "Angkot Surabaya", "code": "ANG", "capacity": 200, "routes": 30},
    ],
    "Bandung": [
        {"mode": "Trans Metro Bandung", "code": "TMB", "capacity": 400, "routes": 4},
        {"mode": "Angkot Bandung", "code": "ANG", "capacity": 150, "routes": 40},
    ],
    "Medan": [
        {"mode": "Bus DAMRI Medan", "code": "DMR", "capacity": 300, "routes": 8},
        {"mode": "Angkot Medan", "code": "ANG", "capacity": 100, "routes": 25},
    ],
    "Makassar": [
        {"mode": "Bus Trans Mamminasata", "code": "TMM", "capacity": 350, "routes": 5},
        {"mode": "Pete-pete", "code": "PP", "capacity": 100, "routes": 30},
    ],
    "Semarang": [
        {"mode": "BRT Trans Semarang", "code": "TS", "capacity": 400, "routes": 7},
        {"mode": "Angkot Semarang", "code": "ANG", "capacity": 120, "routes": 20},
    ],
}


def _ridership_by_hour(hour: int, base: int, fuel_price_surge: bool = False) -> int:
    """
    Simulates ridership based on time of day.
    When fuel_price_surge=True, ridership increases (modal shift effect).
    """
    if 6 <= hour <= 9 or 16 <= hour <= 19:
        factor = random.uniform(0.75, 0.95)
    elif 10 <= hour <= 15:
        factor = random.uniform(0.40, 0.60)
    elif 20 <= hour <= 22:
        factor = random.uniform(0.30, 0.45)
    else:
        factor = random.uniform(0.10, 0.25)

    if fuel_price_surge:
        factor *= random.uniform(1.08, 1.25)  # 8–25% increase in public transport usage

    return min(int(base * factor), base)


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
    )
    logger.info(f"Transport producer started → topic={TOPIC}")

    # Simulate a surge event randomly
    fuel_surge = random.random() < 0.15

    try:
        while True:
            hour = datetime.now().hour
            fuel_surge = random.random() < 0.15  # re-evaluate periodically

            for city, modes in TRANSPORT_SYSTEMS.items():
                for transport in modes:
                    ridership = _ridership_by_hour(hour, transport["capacity"], fuel_surge)
                    load_factor = round(ridership / transport["capacity"] * 100, 2)

                    event = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "city": city,
                        "transport_mode": transport["mode"],
                        "transport_code": transport["code"],
                        "ridership": ridership,
                        "capacity": transport["capacity"],
                        "load_factor_pct": load_factor,
                        "routes_active": transport["routes"],
                        "fuel_price_surge_context": fuel_surge,
                        "source": "simulation",
                    }
                    producer.send(TOPIC, key=f"{city}_{transport['code']}", value=event)

            producer.flush()
            logger.info(f"Produced transport events (fuel_surge={fuel_surge})")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down transport producer")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
