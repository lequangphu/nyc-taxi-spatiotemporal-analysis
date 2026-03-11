import os
from pathlib import Path
from typing import Optional
import tempfile

import polars as pl
import urllib.request
import zipfile


# Determine data directory based on environment
# Streamlit Cloud: use /mount/data (persisted)
# Local: use ./data
# Fallback: use temp directory
if Path("/mount/data").exists():
    DATA_DIR = Path("/mount/data")
else:
    DATA_DIR = Path(__file__).parent.parent.parent.parent / "data"

# Ensure data directory exists
try:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    # If we can't create the data dir, use temp
    DATA_DIR = Path(tempfile.gettempdir()) / "nyc_taxi_data"
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
        return filepath

    url = get_sample_file_url(year, month)

    try:
        # Download with timeout to prevent hanging
        import socket
        socket.setdefaulttimeout(30)  # 30 second timeout

        urllib.request.urlretrieve(url, filepath)
        return filepath
    except Exception as e:
        # If download fails, try temp directory
        if data_dir != Path(tempfile.gettempdir()) / "nyc_taxi_data":
            temp_dir = Path(tempfile.gettempdir()) / "nyc_taxi_data"
            temp_dir.mkdir(exist_ok=True)
            temp_filepath = temp_dir / filename
            if temp_filepath.exists() and not force:
                return temp_filepath
            urllib.request.urlretrieve(url, temp_filepath)
            return temp_filepath
        raise


def load_trip_data(filepath: Path) -> pl.DataFrame:
    """Load taxi trip data from parquet file."""
    df = pl.read_parquet(filepath)
    return df


def get_zone_lookup(data_dir: Path = DATA_DIR) -> pl.DataFrame:
    """Get taxi zone lookup table."""
    zone_file = data_dir / "taxi_zone_lookup.csv"

    if not zone_file.exists():
        url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        try:
            urllib.request.urlretrieve(url, zone_file)
        except Exception:
            # Use temp directory if main data dir fails
            temp_dir = Path(tempfile.gettempdir()) / "nyc_taxi_data"
            temp_dir.mkdir(exist_ok=True)
            zone_file = temp_dir / "taxi_zone_lookup.csv"
            if not zone_file.exists():
                urllib.request.urlretrieve(url, zone_file)

    return pl.read_csv(zone_file)


def get_taxi_zones_shapefile(data_dir: Path = DATA_DIR) -> Optional[Path]:
    """Get taxi zones GeoJSON/shapefile for mapping."""
    return None
