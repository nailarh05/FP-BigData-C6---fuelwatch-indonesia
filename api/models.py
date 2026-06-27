"""Pydantic models for request/response validation."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class FuelPriceEvent(BaseModel):
    timestamp: datetime
    city: str
    fuel_type: str
    price: float = Field(gt=0)
    station: Optional[str] = None
    surge_event: bool = False


class TrafficEvent(BaseModel):
    timestamp: datetime
    city: str
    road_id: str
    road_name: Optional[str] = None
    congestion_level: float = Field(ge=0, le=100)
    avg_speed: float = Field(ge=0)
    mobility_index: float = Field(ge=0, le=100)


class CityMobilityScore(BaseModel):
    city: str
    timestamp: datetime
    mobility_score: float = Field(ge=0, le=1)
    prediction: Optional[float] = None
    traffic_index: float
    fuel_price_delta: float
    weather_score: float
    cluster: Optional[str] = None


class ForecastResponse(BaseModel):
    city: str
    generated_at: datetime
    horizon_hours: int
    steps: list[dict]  # [{"hour_offset": 0.5, "mobility_score": 0.72}, ...]
    model_version: str = "lstm_v1"


class CorrelationResult(BaseModel):
    city: str
    pair: str
    pearson_r: float
    p_value: float
    significant: bool
    direction: str
    strength: str
    best_lag_hours: float


class SurgeAlert(BaseModel):
    city: str
    fuel_type: str
    old_price: float
    new_price: float
    pct_change: float
    severity: str


class DashboardSnapshot(BaseModel):
    city: str
    timestamp: datetime
    mobility_score: float
    congestion_level: float
    fuel_price: float
    weather_score: float
    public_transport_load: float
    impact_level: str
    active_alerts: list[SurgeAlert] = []


class RecommendationResponse(BaseModel):
    city: str
    timestamp: datetime
    best_travel_hour: int
    best_route_tip: str
    expected_congestion: str
    fuel_efficiency_tip: str
    alt_transport_suggestion: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    version: str
    postgres: bool
    redis: bool
    kafka: bool
    timestamp: datetime
