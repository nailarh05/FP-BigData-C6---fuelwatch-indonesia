"""
Fuel Price Producer
-------------------
Produces simulated / scraped fuel price events to the `fuel-price-stream` Kafka topic.
In production: replace _scrape_fuel_prices() with real Pertamina / VIVO / BP scraping.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from loguru import logger

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "fuel-price-stream"
INTERVAL_SEC = 30  # every 30 seconds in dev; set to 300–600 in prod

CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Makassar", "Semarang"]

FUEL_BASE_PRICES: dict[str, float] = {
    "Pertalite": 10_000,
    "Pertamax": 14_000,
    "Pertamax Turbo": 16_600,
    "Dexlite": 15_000,
    "Pertamina Dex": 16_800,
    "Shell Super": 14_590,
    "Vivo Revvo 90": 13_900,
}

STATIONS = ["SPBU Pertamina", "SPBU Shell", "SPBU Vivo", "SPBU BP", "SPBU Total"]


def _scrape_fuel_prices() -> list[dict]:
    """
    Simulates fuel price events.
    Replace with real HTTP scraping or RSS/API calls in production.
    """
    events = []
    # Simulate a price surge event with 10% probability
    surge_active = random.random() < 0.10
    for city in CITIES:
        for fuel_type, base_price in FUEL_BASE_PRICES.items():
            price = base_price
            if surge_active:
                price = round(base_price * random.uniform(1.05, 1.15), -50)  # +5–15%
            else:
                price = round(base_price * random.uniform(0.998, 1.002), -50)

            events.append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "city": city,
                "fuel_type": fuel_type,
                "price": price,
                "station": random.choice(STATIONS),
                "source": "scraper_v1",
                "surge_event": surge_active,
            })
    return events


def _on_send_success(record_metadata):
    logger.debug(
        f"Sent to {record_metadata.topic} "
        f"[partition={record_metadata.partition}, offset={record_metadata.offset}]"
    )


def _on_send_error(exc):
    logger.error(f"Kafka send error: {exc}")


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
    )
    logger.info(f"Fuel price producer started → topic={TOPIC}, bootstrap={KAFKA_BOOTSTRAP}")

    try:
        while True:
            events = _scrape_fuel_prices()
            for event in events:
                producer.send(
                    TOPIC,
                    key=f"{event['city']}_{event['fuel_type']}",
                    value=event,
                ).add_callback(_on_send_success).add_errback(_on_send_error)

            producer.flush()
            logger.info(f"Produced {len(events)} fuel price events")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down fuel price producer")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
