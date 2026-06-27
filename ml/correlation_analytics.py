"""
Correlation Analytics
----------------------
Computes Pearson correlation between:
  1. Harga BBM ↔ Tingkat Mobilitas
  2. Harga BBM ↔ Tingkat Kemacetan
  3. Harga BBM ↔ Penggunaan Transportasi Umum

Uses sliding window correlation to capture lag effects
(price increase today → mobility change in 2–6 hours).

Formula:
  r = Σ(xi - x̄)(yi - ȳ) / sqrt( Σ(xi - x̄)² · Σ(yi - ȳ)² )
"""

import numpy as np
import pandas as pd
from scipy import stats


def pearson_correlation(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    """Returns (r, p_value). p < 0.05 means statistically significant."""
    if len(x) < 3:
        return 0.0, 1.0
    r, p = stats.pearsonr(x, y)
    return float(r), float(p)


def sliding_window_correlation(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    window: int = 48,  # 48 x 30-min = 24 hours
    step: int = 6,
) -> pd.DataFrame:
    """
    Computes Pearson r for each sliding window.
    Returns DataFrame with columns: window_end, r, p_value, significant.
    """
    results = []
    for i in range(window, len(df), step):
        window_df = df.iloc[i - window : i]
        x = window_df[x_col].values
        y = window_df[y_col].values

        # Remove NaN pairs
        mask = ~(np.isnan(x) | np.isnan(y))
        if mask.sum() < 3:
            continue

        r, p = pearson_correlation(x[mask], y[mask])
        results.append({
            "window_end": df.index[i - 1] if isinstance(df.index, pd.DatetimeIndex) else i,
            "r": round(r, 4),
            "p_value": round(p, 6),
            "significant": p < 0.05,
            "n": int(mask.sum()),
        })

    return pd.DataFrame(results)


def lag_correlation(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    max_lag_steps: int = 12,  # test up to 6h lag at 30-min intervals
) -> pd.DataFrame:
    """
    Computes correlation at different time lags to find the strongest relationship.
    Returns DataFrame with columns: lag_steps, lag_hours, r, p_value.
    """
    results = []
    x = df[x_col].values
    for lag in range(0, max_lag_steps + 1):
        if lag == 0:
            x_lag = x
            y_lag = df[y_col].values
        else:
            x_lag = x[:-lag]
            y_lag = df[y_col].values[lag:]

        mask = ~(np.isnan(x_lag) | np.isnan(y_lag))
        if mask.sum() < 10:
            continue

        r, p = pearson_correlation(x_lag[mask], y_lag[mask])
        results.append({
            "lag_steps": lag,
            "lag_hours": lag * 0.5,
            "r": round(r, 4),
            "p_value": round(p, 6),
            "significant": p < 0.05,
        })

    return pd.DataFrame(results)


class FuelMobilityCorrelator:
    """
    Main correlation analytics class.
    Usage:
        corr = FuelMobilityCorrelator()
        corr.fit(df)
        report = corr.generate_report()
    """

    def __init__(self, city: str = "all"):
        self.city = city
        self.results: dict = {}

    def fit(self, df: pd.DataFrame) -> "FuelMobilityCorrelator":
        """
        df must have columns:
          - fuel_price / fuel_price_delta
          - mobility_composite
          - avg_congestion
          - transport_usage_norm
          - window_start (datetime index)
        """
        df = df.sort_values("window_start").set_index("window_start")

        pairs = [
            ("fuel_price_delta", "mobility_composite", "BBM ↔ Mobilitas"),
            ("fuel_price_delta", "avg_congestion", "BBM ↔ Kemacetan"),
            ("fuel_price_delta", "transport_usage_norm", "BBM ↔ Transportasi Umum"),
        ]

        for x_col, y_col, label in pairs:
            if x_col not in df.columns or y_col not in df.columns:
                continue

            overall_r, overall_p = pearson_correlation(
                df[x_col].dropna().values,
                df[y_col].dropna().values,
            )
            sliding = sliding_window_correlation(df.reset_index(), x_col, y_col)
            lag_analysis = lag_correlation(df.reset_index(), x_col, y_col)

            best_lag = lag_analysis.loc[lag_analysis["r"].abs().idxmax()] if not lag_analysis.empty else {}

            self.results[label] = {
                "overall_r": overall_r,
                "overall_p": overall_p,
                "significant": overall_p < 0.05,
                "direction": "negative" if overall_r < 0 else "positive",
                "strength": self._strength_label(abs(overall_r)),
                "sliding_correlation": sliding,
                "lag_analysis": lag_analysis,
                "best_lag_hours": float(best_lag.get("lag_hours", 0)),
                "best_lag_r": float(best_lag.get("r", overall_r)),
            }

        return self

    @staticmethod
    def _strength_label(r_abs: float) -> str:
        if r_abs >= 0.7:
            return "strong"
        elif r_abs >= 0.4:
            return "moderate"
        elif r_abs >= 0.2:
            return "weak"
        return "negligible"

    def generate_report(self) -> pd.DataFrame:
        rows = []
        for label, res in self.results.items():
            rows.append({
                "pair": label,
                "pearson_r": res["overall_r"],
                "p_value": res["overall_p"],
                "significant": res["significant"],
                "direction": res["direction"],
                "strength": res["strength"],
                "best_lag_hours": res["best_lag_hours"],
            })
        return pd.DataFrame(rows)

    def summary(self):
        report = self.generate_report()
        print(f"\n{'='*60}")
        print(f"CORRELATION ANALYSIS — City: {self.city}")
        print(f"{'='*60}")
        print(report.to_string(index=False))
        print()
        for label, res in self.results.items():
            direction_symbol = "↓" if res["direction"] == "negative" else "↑"
            print(f"  {label}:")
            print(f"    r={res['overall_r']:.4f} ({res['strength']}, {res['direction']})")
            print(f"    p={res['overall_p']:.6f} {'✓ sig.' if res['significant'] else '✗ not sig.'}")
            print(f"    Best lag: {res['best_lag_hours']}h → r={res['best_lag_r']:.4f}")


def generate_synthetic_data(city: str, n_steps: int = 500) -> pd.DataFrame:
    """Generate synthetic correlated data for demo."""
    np.random.seed(abs(hash(city)) % (2**32))
    timestamps = pd.date_range("2026-01-01", periods=n_steps, freq="30min")

    fuel_delta = np.random.normal(0, 3, n_steps)
    # Add a price spike
    fuel_delta[200:250] = np.random.uniform(8, 15, 50)

    # Mobility decreases ~0.3 per 10% price increase, with some lag
    lag = 4
    mobility = np.clip(
        0.7 - 0.03 * np.roll(fuel_delta, lag) + np.random.normal(0, 0.05, n_steps),
        0, 1,
    )

    congestion = np.clip(
        0.5 - 0.02 * np.roll(fuel_delta, lag) + np.random.normal(0, 0.08, n_steps),
        0, 1,
    )

    transport = np.clip(
        0.3 + 0.015 * np.roll(fuel_delta, lag) + np.random.normal(0, 0.04, n_steps),
        0, 1,
    )

    return pd.DataFrame({
        "window_start": timestamps,
        "city": city,
        "fuel_price_delta": fuel_delta,
        "mobility_composite": mobility,
        "avg_congestion": congestion,
        "transport_usage_norm": transport,
    })


if __name__ == "__main__":
    for city in ["Jakarta", "Surabaya", "Bandung"]:
        df = generate_synthetic_data(city)
        corr = FuelMobilityCorrelator(city=city)
        corr.fit(df)
        corr.summary()

        report = corr.generate_report()
        report.to_csv(f"/tmp/correlation_report_{city.lower()}.csv", index=False)
        print(f"Report saved → /tmp/correlation_report_{city.lower()}.csv")
