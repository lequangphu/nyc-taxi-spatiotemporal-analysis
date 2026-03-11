import polars as pl
from typing import Dict, Any


def trips_by_hour(df: pl.DataFrame) -> pl.DataFrame:
    """Get trip count by hour of day."""
    return (
        df.group_by("pickup_hour")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
                pl.col("trip_distance").mean().alias("avg_distance"),
            ]
        )
        .sort("pickup_hour")
    )


def trips_by_day_of_week(df: pl.DataFrame) -> pl.DataFrame:
    """Get trip count by day of week."""
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    result = (
        df.group_by("pickup_day_of_week")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("pickup_day_of_week")
    )

    result = result.with_columns(
        [
            pl.col("pickup_day_of_week")
            .map_elements(lambda x: day_names[x - 1], return_dtype=pl.String)
            .alias("day_name")
        ]
    )

    return result


def trips_by_month(df: pl.DataFrame) -> pl.DataFrame:
    """Get trip count by month."""
    return (
        df.group_by("pickup_month")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("pickup_month")
    )


def hourly_patterns_weekday_vs_weekend(df: pl.DataFrame) -> pl.DataFrame:
    """Compare hourly patterns between weekdays and weekends."""
    return (
        df.group_by(["is_weekend", "pickup_hour"])
        .agg([pl.len().alias("trip_count")])
        .sort(["is_weekend", "pickup_hour"])
    )


def get_temporal_summary(df: pl.DataFrame) -> Dict[str, Any]:
    """Get summary statistics for temporal analysis."""
    hour_counts = df.group_by("pickup_hour").agg(pl.len())
    hour_counts = hour_counts.sort("pickup_hour", descending=True)
    return {
        "total_trips": len(df),
        "peak_hour": hour_counts["pickup_hour"][0] if len(hour_counts) > 0 else 0,
        "avg_trip_duration_min": df["trip_duration_minutes"].mean(),
        "avg_fare": df["fare_amount"].mean(),
    }
