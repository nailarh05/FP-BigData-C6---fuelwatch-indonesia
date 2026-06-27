"""
LSTM Mobility Forecasting Model
---------------------------------
Predicts mobility_composite score for the next 24 hours (48 x 30-min steps)
per city, based on:
  - fuel_price_delta
  - traffic_index
  - weather_score_norm
  - transport_usage_norm
  - time_of_day
  - day_of_week
  - holiday_flag

Architecture:
  Input  → LSTM(128) → Dropout(0.2) → LSTM(64) → Dropout(0.2) → Dense(48)
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "2"

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

MODEL_DIR = Path(__file__).parent / "saved_models"
MODEL_DIR.mkdir(exist_ok=True)

FEATURE_COLS = [
    "fuel_price_delta",
    "traffic_index",
    "weather_score_norm",
    "transport_usage_norm",
    "time_of_day_norm",
    "day_of_week_norm",
    "holiday_flag",
]
TARGET_COL = "mobility_composite"
SEQUENCE_LEN = 48   # 48 steps back (24 hours at 30-min interval)
FORECAST_STEPS = 48 # 24 hours ahead


def _normalize_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["time_of_day_norm"] = df["time_of_day"] / 23.0
    df["day_of_week_norm"] = df["day_of_week"] / 6.0
    return df


def build_sequences(df: pd.DataFrame):
    """Convert time-series DataFrame → (X, y) numpy arrays for LSTM."""
    df = _normalize_time_features(df)
    X, y = [], []
    values = df[FEATURE_COLS].values
    targets = df[TARGET_COL].values

    for i in range(SEQUENCE_LEN, len(values) - FORECAST_STEPS):
        X.append(values[i - SEQUENCE_LEN : i])
        y.append(targets[i : i + FORECAST_STEPS])

    return np.array(X, dtype=np.float32), np.array(y, dtype=np.float32)


def build_model(n_features: int = len(FEATURE_COLS), forecast_steps: int = FORECAST_STEPS) -> keras.Model:
    inputs = keras.Input(shape=(SEQUENCE_LEN, n_features), name="sequence_input")

    x = layers.LSTM(128, return_sequences=True, name="lstm_1")(inputs)
    x = layers.Dropout(0.2)(x)
    x = layers.LSTM(64, return_sequences=False, name="lstm_2")(x)
    x = layers.Dropout(0.2)(x)
    x = layers.Dense(64, activation="relu", name="dense_1")(x)
    outputs = layers.Dense(forecast_steps, activation="sigmoid", name="forecast_output")(x)

    model = keras.Model(inputs=inputs, outputs=outputs, name="FuelWatch_LSTM")
    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=1e-3),
        loss="mse",
        metrics=["mae"],
    )
    return model


def train(df_city: pd.DataFrame, city: str, epochs: int = 30, batch_size: int = 32):
    df_city = df_city.sort_values("window_start").reset_index(drop=True)
    df_city[FEATURE_COLS] = df_city[FEATURE_COLS].fillna(0)

    X, y = build_sequences(df_city)
    if len(X) < 10:
        print(f"[{city}] Not enough data to train ({len(X)} sequences). Skipping.")
        return None

    split = int(len(X) * 0.8)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]

    model = build_model()
    model.summary()

    callbacks = [
        keras.callbacks.EarlyStopping(patience=5, restore_best_weights=True),
        keras.callbacks.ReduceLROnPlateau(factor=0.5, patience=3, min_lr=1e-6),
        keras.callbacks.ModelCheckpoint(
            filepath=str(MODEL_DIR / f"lstm_{city.lower()}.keras"),
            save_best_only=True,
        ),
    ]

    history = model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=callbacks,
        verbose=1,
    )

    val_mae = min(history.history["val_mae"])
    print(f"[{city}] Training complete. Best val_MAE: {val_mae:.4f}")
    return model


def predict(df_recent: pd.DataFrame, city: str) -> np.ndarray | None:
    """
    Given the most recent SEQUENCE_LEN rows for a city, predict next 24h.
    Returns array of shape (FORECAST_STEPS,) with mobility scores [0..1].
    """
    model_path = MODEL_DIR / f"lstm_{city.lower()}.keras"
    if not model_path.exists():
        print(f"[{city}] No trained model found at {model_path}")
        return None

    model = keras.models.load_model(str(model_path))
    df_recent = _normalize_time_features(df_recent.copy())
    X = df_recent[FEATURE_COLS].values[-SEQUENCE_LEN:]

    if len(X) < SEQUENCE_LEN:
        print(f"[{city}] Insufficient recent data for prediction.")
        return None

    X = X.reshape(1, SEQUENCE_LEN, len(FEATURE_COLS)).astype(np.float32)
    forecast = model.predict(X, verbose=0)[0]
    return forecast


def generate_synthetic_training_data(city: str, n_days: int = 90) -> pd.DataFrame:
    """
    Generates synthetic time-series data for training/demo.
    Each row = 30-min interval.
    """
    n_steps = n_days * 48
    np.random.seed(abs(hash(city)) % (2**32))

    timestamps = pd.date_range("2026-01-01", periods=n_steps, freq="30min")
    hours = timestamps.hour
    dow = timestamps.dayofweek

    # Rush hours have high traffic_index and low mobility
    rush_mask = ((hours >= 7) & (hours <= 9)) | ((hours >= 17) & (hours <= 19))
    traffic_index = np.where(rush_mask, np.random.uniform(0.6, 0.95, n_steps), np.random.uniform(0.1, 0.5, n_steps))

    # Fuel price increases mid-simulation
    base_price_delta = np.zeros(n_steps)
    spike_start = n_steps // 2
    base_price_delta[spike_start:spike_start + 200] = np.random.uniform(5, 15, 200)

    weather_score = np.random.uniform(0.3, 1.0, n_steps)
    transport_usage = np.clip(1 - traffic_index + np.random.normal(0, 0.1, n_steps), 0, 1)
    holiday_flag = (dow >= 5).astype(float)

    mobility = (
        (1 - traffic_index) * 0.40
        + weather_score * 0.30
        + transport_usage * 0.20
        + np.clip(1 - base_price_delta / 100, 0, 1) * 0.10
        + np.random.normal(0, 0.02, n_steps)
    ).clip(0, 1)

    return pd.DataFrame({
        "window_start": timestamps,
        "city": city,
        "fuel_price_delta": base_price_delta,
        "traffic_index": traffic_index,
        "weather_score_norm": weather_score,
        "transport_usage_norm": transport_usage,
        "time_of_day": hours,
        "day_of_week": dow,
        "holiday_flag": holiday_flag,
        TARGET_COL: mobility,
    })


if __name__ == "__main__":
    CITIES = ["Jakarta", "Surabaya", "Bandung", "Medan", "Makassar", "Semarang"]
    for city in CITIES:
        print(f"\n{'='*50}")
        print(f"Training LSTM for: {city}")
        df = generate_synthetic_training_data(city, n_days=90)
        train(df, city, epochs=20)

    # Quick inference demo
    df_recent = generate_synthetic_training_data("Jakarta", n_days=5).tail(48)
    forecast = predict(df_recent, "Jakarta")
    if forecast is not None:
        print(f"\nJakarta 24h mobility forecast (first 6 steps):")
        for i, val in enumerate(forecast[:6]):
            print(f"  +{(i+1)*30}min → mobility_score={val:.3f}")
