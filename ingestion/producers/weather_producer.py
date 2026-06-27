"""
Weather Producer
----------------
Fetches weather data from OpenWeatherMap and streams to `weather-stream`.
Weather is a confounding variable — rain reduces mobility independently of fuel price.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from loguru import logger

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
OWM_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
TOPIC = "weather-stream"
INTERVAL_SEC = 300  # weather changes slowly, every 5 min is enough

CITY_COORDS: dict[str, tuple[float, float]] = {
    "Jakarta": (-6.2088, 106.8456),
    "Surabaya": (-7.2575, 112.7521),
    "Bandung": (-6.9175, 107.6191),
    "Medan": (3.5952, 98.6722),
    "Makassar": (-5.1477, 119.4327),
    "Semarang": (-6.9932, 110.4203),
}


def _fetch_owm_weather(city: str, lat: float, lon: float) -> dict | None:
    if not OWM_API_KEY or OWM_API_KEY == "demo_key":
        return None
    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?lat={lat}&lon={lon}&appid={OWM_API_KEY}&units=metric"
    )
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        return {
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather_main": data["weather"][0]["main"],
            "weather_desc": data["weather"][0]["description"],
            "wind_speed": data["wind"]["speed"],
            "rain_1h": data.get("rain", {}).get("1h", 0.0),
        }
    except Exception as exc:
        logger.warning(f"OWM API error for {city}: {exc}")
        return None


def _weather_score(weather_main: str, rain_1h: float) -> float:
    """
    Score 0–100: 100 = perfect weather (no rain), 0 = severe weather.
    Used as input feature for mobility prediction.
    """
    base = {"Clear": 100, "Clouds": 80, "Drizzle": 60, "Rain": 40, "Thunderstorm": 10, "Fog": 50}
    score = base.get(weather_main, 70)
    score -= min(rain_1h * 5, 30)
    return max(0, round(score, 1))


def _simulate_weather(city: str) -> dict:
    weather_options = [
        ("Clear", "clear sky", 0.0),
        ("Clouds", "scattered clouds", 0.0),
        ("Rain", "moderate rain", random.uniform(1, 10)),
        ("Drizzle", "light drizzle", random.uniform(0.1, 1)),
        ("Thunderstorm", "thunderstorm with rain", random.uniform(5, 20)),
    ]
    w_main, w_desc, rain = random.choices(
        weather_options, weights=[40, 30, 15, 10, 5]
    )[0]

    return {
        "temperature": random.uniform(25, 35),
        "humidity": random.uniform(60, 90),
        "weather_main": w_main,
        "weather_desc": w_desc,
        "wind_speed": random.uniform(0, 10),
        "rain_1h": rain,
    }


def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
    )
    logger.info(f"Weather producer started → topic={TOPIC}")

    try:
        while True:
            for city, (lat, lon) in CITY_COORDS.items():
                raw = _fetch_owm_weather(city, lat, lon) or _simulate_weather(city)
                event = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "city": city,
                    "lat": lat,
                    "lon": lon,
                    **raw,
                    "weather_score": _weather_score(raw["weather_main"], raw["rain_1h"]),
                    "source": "openweathermap" if OWM_API_KEY else "simulation",
                }
                producer.send(TOPIC, key=city, value=event)
                logger.debug(f"Weather event: {city} → {event['weather_main']}, score={event['weather_score']}")

            producer.flush()
            logger.info("Produced weather events for all cities")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down weather producer")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
