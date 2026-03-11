# Streamlit entry point - use this for local development or alternative deployment

# Page config MUST be first Streamlit command
import streamlit as st

st.set_page_config(
    page_title="NYC Taxi Spatiotemporal Analysis", page_icon="🚕", layout="wide"
)

import sys
from pathlib import Path

# Add src to path for local development
src_path = Path(__file__).parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from nyc_taxi_spatiotemporal_analysis.dashboard.app import run_app

if __name__ == "__main__":
    run_app()
