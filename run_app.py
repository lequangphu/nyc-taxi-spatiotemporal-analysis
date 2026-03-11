#!/usr/bin/env python3
"""
Convenient entry point for running the NYC Taxi Streamlit dashboard.
This script handles Python path configuration automatically.

Usage:
    streamlit run run_app.py
"""

import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Import the dashboard - Streamlit will execute it
from src.dashboard import app
