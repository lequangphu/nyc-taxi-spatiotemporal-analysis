import os
from pathlib import Path
from typing import Optional

import polars as pl
import urllib.request
import zipfile


DATA_DIR = Path(__file__).parent.parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_sample_file_url(year: int = 2024, month: int = 1) -> str:
    """Get URL for NYC TLC Yellow Taxi sample data."""
    return f"{NYC_TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_sample(
    year: int = 2024,
    month: int = 1,
    data_dir: Path = DATA_DIR,
    force: bool = False,
) -> Path:
    """Download sample taxi trip data from NYC TLC."""
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    filepath = data_dir / filename

    if filepath.exists() and not force:
        print(f"Data already exists at {filepath}")
        return filepath

    url = get_sample_file_url(year, month)
    print(f"Downloading {url}...")

    urllib.request.urlretrieve(url, filepath)
    print(f"Downloaded to {filepath}")

    return filepath


def load_trip_data(filepath: Path) -> pl.DataFrame:
    """Load taxi trip data from parquet file."""
    df = pl.read_parquet(filepath)
    return df


def get_zone_lookup(data_dir: Path = DATA_DIR) -> pl.DataFrame:
    """Get taxi zone lookup table."""
    zone_file = data_dir / "taxi_zone_lookup.csv"

    if not zone_file.exists():
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        print(f"Downloading zone lookup from {url}...")
        urllib.request.urlretrieve(url, zone_file)

    return pl.read_csv(zone_file)


def get_taxi_zones_shapefile(data_dir: Path = DATA_DIR) -> Optional[Path]:
    """Get taxi zones GeoJSON/shapefile for mapping."""
    return None
