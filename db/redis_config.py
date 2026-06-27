"""
Redis Cache Layer
-----------------
Provides fast read/write for:
  - Real-time dashboard data (TTL 30s)
  - Latest city mobility scores (TTL 60s)
  - Fuel price alerts (TTL 5min)
  - Prediction cache (TTL 30min)
  - Hot analytics (sliding window stats)
"""

import json
import os
from datetime import timedelta
from typing import Any

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Key prefixes
K_FUEL_LATEST = "fuel:latest:{city}:{fuel_type}"
K_TRAFFIC_LATEST = "traffic:latest:{city}"
K_MOBILITY_SCORE = "mobility:score:{city}"
K_PREDICTION = "prediction:{city}:{horizon_h}"
K_CORRELATION = "correlation:{city}"
K_ALERT = "alert:fuel_surge:{city}"
K_DASHBOARD_SNAPSHOT = "dashboard:snapshot:{city}"
K_CITY_CLUSTER = "cluster:{city}"

# TTLs
TTL_REALTIME = 30        # 30s for live dashboard data
TTL_MOBILITY = 60        # 1 min for mobility score
TTL_PREDICTION = 1800    # 30 min for forecast
TTL_ALERT = 300          # 5 min for surge alerts
TTL_CORRELATION = 3600   # 1h for correlation results
TTL_CLUSTER = 86400      # 24h for cluster assignments


class FuelWatchCache:
    def __init__(self, url: str = REDIS_URL):
        self.client = redis.from_url(url, decode_responses=True)

    def ping(self) -> bool:
        try:
            return self.client.ping()
        except redis.ConnectionError:
            return False

    # ── Fuel Price ────────────────────────────────────────────────────────────

    def set_fuel_price(self, city: str, fuel_type: str, price: float, timestamp: str):
        key = K_FUEL_LATEST.format(city=city, fuel_type=fuel_type.replace(" ", "_"))
        value = json.dumps({"price": price, "timestamp": timestamp, "city": city, "fuel_type": fuel_type})
        self.client.setex(key, TTL_REALTIME, value)

    def get_fuel_price(self, city: str, fuel_type: str) -> dict | None:
        key = K_FUEL_LATEST.format(city=city, fuel_type=fuel_type.replace(" ", "_"))
        data = self.client.get(key)
        return json.loads(data) if data else None

    def get_all_fuel_prices(self, city: str) -> list[dict]:
        pattern = K_FUEL_LATEST.format(city=city, fuel_type="*")
        keys = self.client.keys(pattern)
        results = []
        for k in keys:
            data = self.client.get(k)
            if data:
                results.append(json.loads(data))
        return results

    # ── Traffic ───────────────────────────────────────────────────────────────

    def set_traffic(self, city: str, data: dict):
        key = K_TRAFFIC_LATEST.format(city=city)
        self.client.setex(key, TTL_REALTIME, json.dumps(data))

    def get_traffic(self, city: str) -> dict | None:
        key = K_TRAFFIC_LATEST.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── Mobility Score ────────────────────────────────────────────────────────

    def set_mobility_score(self, city: str, score: float, timestamp: str):
        key = K_MOBILITY_SCORE.format(city=city)
        self.client.setex(key, TTL_MOBILITY, json.dumps({"score": score, "timestamp": timestamp}))

    def get_mobility_score(self, city: str) -> dict | None:
        key = K_MOBILITY_SCORE.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── Predictions ───────────────────────────────────────────────────────────

    def set_prediction(self, city: str, horizon_h: int, forecast: list[float]):
        key = K_PREDICTION.format(city=city, horizon_h=horizon_h)
        self.client.setex(key, TTL_PREDICTION, json.dumps(forecast))

    def get_prediction(self, city: str, horizon_h: int) -> list[float] | None:
        key = K_PREDICTION.format(city=city, horizon_h=horizon_h)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── Alerts ────────────────────────────────────────────────────────────────

    def set_surge_alert(self, city: str, fuel_type: str, old_price: float, new_price: float):
        key = K_ALERT.format(city=city)
        pct_change = round((new_price - old_price) / old_price * 100, 2)
        alert = {
            "city": city,
            "fuel_type": fuel_type,
            "old_price": old_price,
            "new_price": new_price,
            "pct_change": pct_change,
            "severity": "high" if pct_change > 10 else "medium",
        }
        self.client.setex(key, TTL_ALERT, json.dumps(alert))

    def get_surge_alert(self, city: str) -> dict | None:
        key = K_ALERT.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    def get_all_alerts(self) -> list[dict]:
        keys = self.client.keys("alert:fuel_surge:*")
        results = []
        for k in keys:
            data = self.client.get(k)
            if data:
                results.append(json.loads(data))
        return results

    # ── Dashboard Snapshot ────────────────────────────────────────────────────

    def set_dashboard_snapshot(self, city: str, snapshot: dict):
        key = K_DASHBOARD_SNAPSHOT.format(city=city)
        self.client.setex(key, TTL_REALTIME, json.dumps(snapshot))

    def get_dashboard_snapshot(self, city: str) -> dict | None:
        key = K_DASHBOARD_SNAPSHOT.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── Correlation ───────────────────────────────────────────────────────────

    def set_correlation(self, city: str, results: dict):
        key = K_CORRELATION.format(city=city)
        self.client.setex(key, TTL_CORRELATION, json.dumps(results))

    def get_correlation(self, city: str) -> dict | None:
        key = K_CORRELATION.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── City Cluster ──────────────────────────────────────────────────────────

    def set_city_cluster(self, city: str, cluster: int, impact_level: str):
        key = K_CITY_CLUSTER.format(city=city)
        self.client.setex(key, TTL_CLUSTER, json.dumps({"cluster": cluster, "impact_level": impact_level}))

    def get_city_cluster(self, city: str) -> dict | None:
        key = K_CITY_CLUSTER.format(city=city)
        data = self.client.get(key)
        return json.loads(data) if data else None

    # ── Sliding Window Analytics (sorted sets) ────────────────────────────────

    def add_to_timeseries(self, metric: str, city: str, value: float, score: float):
        """Store time-series point using sorted set (score = unix timestamp)."""
        key = f"ts:{metric}:{city}"
        self.client.zadd(key, {json.dumps({"v": value}): score})
        self.client.expire(key, 86400)  # keep 24h

    def get_timeseries(self, metric: str, city: str, start: float, end: float) -> list[tuple]:
        key = f"ts:{metric}:{city}"
        items = self.client.zrangebyscore(key, start, end, withscores=True)
        return [(json.loads(item), score) for item, score in items]

    # ── Health ────────────────────────────────────────────────────────────────

    def health(self) -> dict:
        info = self.client.info()
        return {
            "connected": True,
            "used_memory_human": info.get("used_memory_human"),
            "connected_clients": info.get("connected_clients"),
            "total_keys": self.client.dbsize(),
        }
