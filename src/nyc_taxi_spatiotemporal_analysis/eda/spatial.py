import polars as pl
from typing import Dict, Any


def top_pickup_zones(df: pl.DataFrame, n: int = 10) -> pl.DataFrame:
    """Get top N pickup zones by trip count."""
    return (
        df.group_by("PULocationID")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").sum().alias("total_fare"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("trip_count", descending=True)
        .head(n)
    )


def top_dropoff_zones(df: pl.DataFrame, n: int = 10) -> pl.DataFrame:
    """Get top N dropoff zones by trip count."""
    return (
        df.group_by("DOLocationID")
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").sum().alias("total_fare"),
                pl.col("fare_amount").mean().alias("avg_fare"),
            ]
        )
        .sort("trip_count", descending=True)
        .head(n)
    )


def zone_pair_flows(df: pl.DataFrame, n: int = 10) -> pl.DataFrame:
    """Get most common pickup-dropoff zone pairs."""
    return (
        df.group_by(["PULocationID", "DOLocationID"])
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("fare_amount").mean().alias("avg_fare"),
                pl.col("trip_distance").mean().alias("avg_distance"),
            ]
        )
        .sort("trip_count", descending=True)
        .head(n)
    )


def get_spatial_summary(df: pl.DataFrame) -> Dict[str, Any]:
    """Get summary statistics for spatial analysis."""
    return {
        "unique_pickup_zones": df["PULocationID"].n_unique(),
        "unique_dropoff_zones": df["DOLocationID"].n_unique(),
        "avg_trip_distance": df["trip_distance"].mean(),
        "median_trip_distance": df["trip_distance"].median(),
    }


def zone_heatmap_data(df: pl.DataFrame) -> pl.DataFrame:
    """Get data for zone-hour heatmap."""
    return (
        df.group_by(["PULocationID", "pickup_hour"])
        .agg([pl.len().alias("trip_count")])
        .sort(["PULocationID", "pickup_hour"])
    )
