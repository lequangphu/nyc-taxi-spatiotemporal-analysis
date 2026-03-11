import polars as pl
from typing import Dict, Any


def get_basic_stats(df: pl.DataFrame) -> Dict[str, Any]:
    """Get basic statistics for the dataset."""
    return {
        "total_trips": len(df),
        "date_range": {
            "start": df["tpep_pickup_datetime"].min(),
            "end": df["tpep_pickup_datetime"].max(),
        },
        "fare": {
            "mean": df["fare_amount"].mean(),
            "median": df["fare_amount"].median(),
            "std": df["fare_amount"].std(),
            "min": df["fare_amount"].min(),
            "max": df["fare_amount"].max(),
        },
        "distance": {
            "mean": df["trip_distance"].mean(),
            "median": df["trip_distance"].median(),
            "std": df["trip_distance"].std(),
            "min": df["trip_distance"].min(),
            "max": df["trip_distance"].max(),
        },
        "duration": {
            "mean": df["trip_duration_minutes"].mean(),
            "median": df["trip_duration_minutes"].median(),
            "std": df["trip_duration_minutes"].std(),
            "min": df["trip_duration_minutes"].min(),
            "max": df["trip_duration_minutes"].max(),
        },
    }


def get_passenger_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Get statistics by passenger count."""
    return (
        df.group_by("passenger_count")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
                pl.col("trip_distance").mean().alias("avg_distance"),
            ]
        )
        .sort("passenger_count")
    )


def get_payment_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Get statistics by payment type."""
    return (
        df.group_by("payment_type")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").sum().alias("total_fare"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("trip_count", descending=True)
    )


def get_ratecode_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Get statistics by rate code."""
    return (
        df.group_by("RatecodeID")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
                pl.col("trip_distance").mean().alias("avg_distance"),
            ]
        )
        .sort("trip_count", descending=True)
    )


def get_vendor_stats(df: pl.DataFrame) -> pl.DataFrame:
    """Get statistics by vendor."""
    return (
        df.group_by("VendorID")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("trip_count", descending=True)
    )
