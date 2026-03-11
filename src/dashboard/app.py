# Redirect to the new package location
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from nyc_taxi_spatiotemporal_analysis.dashboard.app import main

if __name__ == "__main__":
    main()
