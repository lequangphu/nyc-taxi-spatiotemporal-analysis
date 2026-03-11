import polars as pl
from polars import Schema
from typing import Optional


def add_temporal_features(df: pl.DataFrame) -> pl.DataFrame:
    """Add temporal features from pickup datetime."""
    df = df.with_columns(
        [
            pl.col("tpep_pickup_datetime").dt.hour().alias("pickup_hour"),
            pl.col("tpep_pickup_datetime").dt.weekday().alias("pickup_day_of_week"),
            pl.col("tpep_pickup_datetime").dt.day().alias("pickup_day"),
            pl.col("tpep_pickup_datetime").dt.month().alias("pickup_month"),
            pl.col("tpep_pickup_datetime").dt.year().alias("pickup_year"),
        ]
    )

    df = df.with_columns(
        [
            pl.when(pl.col("pickup_day_of_week") >= 5)
            .then(pl.lit("Weekend"))
            .otherwise(pl.lit("Weekday"))
            .alias("is_weekend")
        ]
    )

    return df


def add_trip_duration(df: pl.DataFrame) -> pl.DataFrame:
    """Add trip duration in minutes."""
    df = df.with_columns(
        [
            (pl.col("tpep_dropoff_datetime") - pl.col("tpep_pickup_datetime"))
            .dt.total_minutes()
            .alias("trip_duration_minutes")
        ]
    )
    return df


def add_trip_speed(df: pl.DataFrame) -> pl.DataFrame:
    """Add trip speed in mph."""
    df = df.with_columns(
        [
            (pl.col("trip_distance") / (pl.col("trip_duration_minutes") / 60)).alias(
                "trip_speed_mph"
            )
        ]
    )
    return df


def add_fare_per_mile(df: pl.DataFrame) -> pl.DataFrame:
    """Add fare per mile calculation."""
    df = df.with_columns(
        [(pl.col("fare_amount") / pl.col("trip_distance")).alias("fare_per_mile")]
    )
    return df


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    """Clean data by removing invalid records."""
    initial_count = len(df)

    df = df.filter(pl.col("fare_amount") > 0)
    df = df.filter(pl.col("trip_distance") >= 0)
    df = df.filter(pl.col("trip_duration_minutes") > 0)
    df = df.filter(pl.col("trip_duration_minutes") < 180)
    df = df.filter(pl.col("PULocationID").is_not_null())
    df = df.filter(pl.col("DOLocationID").is_not_null())

    removed = initial_count - len(df)
    if removed > 0:
        print(
            f"Removed {removed} invalid records ({removed / initial_count * 100:.1f}%)"
        )

    return df


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """Apply all transformations."""
    df = add_temporal_features(df)
    df = add_trip_duration(df)
    df = add_trip_speed(df)
    df = add_fare_per_mile(df)
    df = clean_data(df)
    return df
