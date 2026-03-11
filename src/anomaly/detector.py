import polars as pl
from typing import Tuple, List


MAX_REASONABLE_SPEED_MPH = 60.0
MIN_REASONABLE_SPEED_MPH = 0.0
MAX_REASONABLE_FARE = 1000.0
MAX_REASONABLE_DISTANCE = 100.0
MAX_REASONABLE_DURATION_MIN = 180.0
MIN_REASONABLE_FARE = 0.0


def detect_speed_anomalies(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Detect trips with unrealistic speeds."""
    df_with_speed = df.filter(
        pl.col("trip_speed_mph").is_not_null()
        & (pl.col("trip_speed_mph") > MAX_REASONABLE_SPEED_MPH)
    )
    df_normal = df.filter(
        pl.col("trip_speed_mph").is_null()
        | (pl.col("trip_speed_mph") <= MAX_REASONABLE_SPEED_MPH)
    )
    return df_with_speed, df_normal


def detect_fare_anomalies(
    df: pl.DataFrame, iqr_multiplier: float = 3.0
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Detect fare outliers using IQR method."""
    q1 = df["fare_amount"].quantile(0.25)
    q3 = df["fare_amount"].quantile(0.75)
    iqr = q3 - q1
    upper_bound = q3 + iqr_multiplier * iqr

    df_anomaly = df.filter(pl.col("fare_amount") > upper_bound)
    df_normal = df.filter(pl.col("fare_amount") <= upper_bound)
    return df_anomaly, df_normal


def detect_short_long_trips(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Detect trips that are suspiciously short distance but long duration."""
    df_anomaly = df.filter(
        (pl.col("trip_distance") < 0.1) & (pl.col("trip_duration_minutes") > 30)
    )
    df_normal = df.filter(
        ~((pl.col("trip_distance") < 0.1) & (pl.col("trip_duration_minutes") > 30))
    )
    return df_anomaly, df_normal


def detect_long_short_trips(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Detect trips that are long distance but suspiciously short duration."""
    df_anomaly = df.filter(
        (pl.col("trip_distance") > 20) & (pl.col("trip_duration_minutes") < 10)
    )
    df_normal = df.filter(
        ~((pl.col("trip_distance") > 20) & (pl.col("trip_duration_minutes") < 10))
    )
    return df_anomaly, df_normal


def detect_late_night_high_fare(
    df: pl.DataFrame, hour: int = 22, fare_threshold: float = 100.0
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Detect unusually high fares during late night hours."""
    df_anomaly = df.filter(
        (pl.col("pickup_hour") >= hour) & (pl.col("fare_amount") > fare_threshold)
    )
    df_normal = df.filter(
        ~((pl.col("pickup_hour") >= hour) & (pl.col("fare_amount") > fare_threshold))
    )
    return df_anomaly, df_normal


def detect_zone_anomalies(
    df: pl.DataFrame, zone_col: str = "PULocationID", iqr_multiplier: float = 3.0
) -> dict:
    """Detect anomalies in fare by zone."""
    results = {}
    zones = df[zone_col].unique().to_list()

    for zone in zones:
        zone_df = df.filter(pl.col(zone_col) == zone)
        if len(zone_df) < 10:
            continue

        q1 = zone_df["fare_amount"].quantile(0.25)
        q3 = zone_df["fare_amount"].quantile(0.75)
        iqr = q3 - q1
        upper_bound = q3 + iqr_multiplier * iqr

        zone_anomalies = zone_df.filter(pl.col("fare_amount") > upper_bound)
        results[zone] = {
            "count": len(zone_anomalies),
            "total_anomaly_fare": zone_anomalies["fare_amount"].sum(),
        }

    return results


def detect_all_anomalies(df: pl.DataFrame) -> Tuple[pl.DataFrame, dict]:
    """Run all anomaly detection methods and return summary."""
    summary = {}

    df_speed, df = detect_speed_anomalies(df)
    summary["speed_anomalies"] = len(df_speed)

    df_fare, df = detect_fare_anomalies(df)
    summary["fare_anomalies"] = len(df_fare)

    df_short_long, df = detect_short_long_trips(df)
    summary["short_long_anomalies"] = len(df_short_long)

    df_long_short, df = detect_long_short_trips(df)
    summary["long_short_anomalies"] = len(df_long_short)

    df_late_night, df = detect_late_night_high_fare(df)
    summary["late_night_high_fare"] = len(df_late_night)

    df_all_anomalies = pl.concat(
        [df_speed, df_fare, df_short_long, df_long_short, df_late_night]
    )
    df_all_anomalies = df_all_anomalies.unique()

    summary["total_anomalies"] = len(df_all_anomalies)
    summary["anomaly_rate"] = len(df_all_anomalies) / len(df) * 100

    return df_all_anomalies, summary
