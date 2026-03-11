import sys
from pathlib import Path

# Add src to path for local development
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from nyc_taxi_spatiotemporal_analysis.dashboard.app import main

if __name__ == "__main__":
    main()
