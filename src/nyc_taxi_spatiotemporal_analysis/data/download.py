import os
from pathlib import Path
from typing import Optional
import datetime

import polars as pl


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
    import tempfile
    DATA_DIR = Path(tempfile.gettempdir()) / "nyc_taxi_data"
    DATA_DIR.mkdir(exist_ok=True)

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_sample_file_url(year: int = 2024, month: int = 1) -> str:
    """Get URL for NYC TLC Yellow Taxi sample data."""
    return f"{NYC_TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def create_embedded_sample(data_dir: Path = DATA_DIR, num_rows: int = 50000) -> Path:
    """Create a realistic embedded sample dataset for reliable operation.

    This generates a sample dataset that mimics NYC taxi data patterns
    without requiring large downloads from external sources.
    """
    filepath = data_dir / f"nyc_taxi_sample_{num_rows}.parquet"

    if filepath.exists():
        return filepath

    import random

    # Generate realistic sample data
    random.seed(42)

    # Time range across different times of day
    base_time = datetime.datetime(2024, 1, 15, 0, 0, 0)
    pickup_times = [
        base_time + datetime.timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        for _ in range(num_rows)
    ]

    # Trip durations: 5-60 minutes typically
    durations = [random.randint(5, 60) for _ in range(num_rows)]
    dropoff_times = [pickup_times[i] + datetime.timedelta(minutes=durations[i])
                     for i in range(num_rows)]

    # Locations: NYC taxi zones (sample of real zones)
    manhattan_zones = list(range(1, 64)) + list(range(100, 164))
    queens_zones = list(range(1, 10)) + list(range(130, 140))
    brooklyn_zones = list(range(20, 50)) + list(range(100, 120))
    bronx_zones = list(range(200, 220))
    all_zones = manhattan_zones + queens_zones + brooklyn_zones + bronx_zones

    # Passenger counts: 0-6 (0 = street hails with unknown passengers)
    passenger_counts = [random.choice([0, 1, 1, 1, 2, 2, 3, 4, 5, 6]) for _ in range(num_rows)]

    # Trip distances: 0.5 to 20 miles
    trip_distances = [round(random.uniform(0.5, 20.0), 2) for _ in range(num_rows)]

    # Fare amounts: correlated with distance
    fare_amounts = [round(3.0 + (d * 2.5) + random.uniform(-2, 5), 2) for d in trip_distances]

    # Generate sample data
    sample_data = {
        "VendorID": [random.choice([1, 2]) for _ in range(num_rows)],
        "tpep_pickup_datetime": pickup_times,
        "tpep_dropoff_datetime": dropoff_times,
        "passenger_count": passenger_counts,
        "trip_distance": trip_distances,
        "RatecodeID": [random.choice([1, 1, 1, 1, 2, 3, 4, 5, 6]) for _ in range(num_rows)],
        "store_and_fwd_flag": [random.choice(["N", "N", "N", "N", "Y"]) for _ in range(num_rows)],
        "PULocationID": [random.choice(all_zones) for _ in range(num_rows)],
        "DOLocationID": [random.choice(all_zones) for _ in range(num_rows)],
        "payment_type": [random.choice([1, 1, 1, 2, 3, 4]) for _ in range(num_rows)],
        "fare_amount": fare_amounts,
        "extra": [random.choice([0, 0, 0, 0.5, 1.0]) for _ in range(num_rows)],
        "mta_tax": [0.5 for _ in range(num_rows)],
        "tip_amount": [round(f * random.uniform(0.15, 0.25), 2) for f in fare_amounts],
        "tolls_amount": [random.choice([0, 0, 0, 0, 3.0, 5.5]) for _ in range(num_rows)],
        "improvement_surcharge": [1.0 for _ in range(num_rows)],
        "total_amount": [round(f + 0.5 + 1.0 + f*0.2 + random.choice([0, 0, 3.0]), 2) for f in fare_amounts],
        "congestion_surcharge": [2.5 if random.random() > 0.3 else 0.0 for _ in range(num_rows)],
        "Airport_fee": [0.0 for _ in range(num_rows)],
    }

    df = pl.DataFrame(sample_data)
    df.write_parquet(filepath)

    return filepath


def download_sample(
    year: int = 2024,
    month: int = 1,
    data_dir: Path = DATA_DIR,
    force: bool = False,
    max_rows: int = 100000,
) -> Path:
    """Get sample taxi trip data.

    For reliability on Streamlit Cloud, uses embedded sample data.
    Set force=True to regenerate the sample.
    """
    filename = f"yellow_tripdata_{year}-{month:02d}_sample{max_rows}.parquet"
    filepath = data_dir / filename

    if filepath.exists() and not force:
        return filepath

    # Generate embedded sample instead of downloading
    # This is much more reliable for Streamlit Cloud
    return create_embedded_sample(data_dir, num_rows=min(max_rows, 50000))


def load_trip_data(filepath: Path) -> pl.DataFrame:
    """Load taxi trip data from parquet file."""
    df = pl.read_parquet(filepath)
    return df


def get_zone_lookup(data_dir: Path = DATA_DIR) -> pl.DataFrame:
    """Get taxi zone lookup table."""
    zone_file = data_dir / "taxi_zone_lookup.csv"

    if not zone_file.exists():
        # Create a minimal zone lookup
        zone_data = []
        for i in range(1, 266):
            if i < 64:
                borough = "Manhattan"
                zone = f"Manhattan Zone {i}"
            elif i < 100:
                borough = "Queens"
                zone = f"Queens Zone {i}"
            elif i < 200:
                borough = "Brooklyn"
                zone = f"Brooklyn Zone {i}"
            elif i < 264:
                borough = "Bronx"
                zone = f"Bronx Zone {i}"
            else:
                borough = "Staten Island"
                zone = f"Staten Island Zone {i}"

            zone_data.append({
                "LocationID": i,
                "Borough": borough,
                "Zone": zone,
                "service_zone": "Yellow Zone",
            })

        df = pl.DataFrame(zone_data)
        df.write_csv(zone_file)

    return pl.read_csv(zone_file)


def get_taxi_zones_shapefile(data_dir: Path = DATA_DIR) -> Optional[Path]:
    """Get taxi zones GeoJSON/shapefile for mapping."""
    return None
