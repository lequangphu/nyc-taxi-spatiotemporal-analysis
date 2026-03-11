# Streamlit Cloud entry point - redirects to the new package location
# This file is maintained for backward compatibility with existing Streamlit Cloud config

# Page config MUST be first Streamlit command
import streamlit as st

st.set_page_config(
    page_title="NYC Taxi Spatiotemporal Analysis", page_icon="🚕", layout="wide"
)

import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import and run the actual dashboard
from nyc_taxi_spatiotemporal_analysis.dashboard.app import run_app

if __name__ == "__main__":
    run_app()
