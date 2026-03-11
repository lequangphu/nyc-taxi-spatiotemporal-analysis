# NYC Taxi Spatiotemporal Analysis

A comprehensive spatiotemporal data analysis project demonstrating EDA, zone grouping, and behavior-based anomaly detection on NYC Yellow Taxi trip data.

## Project Overview

This project demonstrates skills relevant to the Data Analyst position at Hatonet, focusing on:
- **Spatiotemporal EDA**: Analyzing patterns across time and space
- **Device/Zone Grouping**: Grouping 265 taxi zones into logical regions
- **Behavior-based Anomaly Detection**: Identifying unusual trip patterns

## Features

- **Data Pipeline**: Automated download and processing of NYC TLC taxi trip data
- **Temporal Analysis**: Hourly, daily, and monthly patterns
- **Spatial Analysis**: Zone-level pickup/dropoff patterns
- **Zone Grouping**: Grouping taxi zones into regions (Manhattan, Brooklyn, Queens, Bronx, Staten Island, Airports)
- **Anomaly Detection**: Speed anomalies, fare outliers, unusual trip patterns
- **Interactive Dashboard**: Streamlit-powered visualization

## Tech Stack

- **Python**: Data processing
- **Polars**: High-performance DataFrame operations
- **Streamlit**: Interactive dashboard
- **Plotly**: Interactive visualizations

## Quick Start

### 1. Install Dependencies

```bash
pip install -e .
```

Or with uv:

```bash
uv sync
```

### 2. Run the Dashboard

```bash
streamlit run src/dashboard/app.py
```

The dashboard will open at `http://localhost:8501`.

### 3. Select Data

Use the sidebar to select year and month of taxi data to analyze.

## Project Structure

```
nyc-taxi-spatiotemporal-analysis/
├── src/
│   ├── data/
│   │   ├── download.py     # Data download from NYC TLC
│   │   └── transform.py    # Data transformations
│   ├── eda/
│   │   ├── temporal.py     # Temporal analysis
│   │   ├── spatial.py      # Spatial analysis
│   │   └── stats.py        # Statistical summaries
│   ├── zones/
│   │   └── grouper.py      # Zone grouping logic
│   ├── anomaly/
│   │   └── detector.py     # Anomaly detection
│   └── dashboard/
│       └── app.py          # Streamlit dashboard
├── data/                   # Data storage
├── pyproject.toml
└── README.md
```

## Key Findings

### Temporal Patterns
- Peak hours: 5-7 PM (evening rush)
- Lowest activity: 3-5 AM
- Weekdays show higher activity than weekends

### Spatial Patterns
- Manhattan dominates pickup/dropoff activity
- JFK and LaGuardia airports have distinct patterns
- Inter-borough flows show clear commuter patterns

### Anomaly Detection
- ~2-5% of trips flagged as anomalous
- Common anomalies: speed violations, fare outliers, unusual duration patterns
- Late night (10PM-3AM) shows higher anomaly rates

## Deployment

Deploy to Streamlit Cloud:

1. Push to GitHub
2. Connect repository to [Streamlit Cloud](https://share.streamlit.io)
3. Set main file to `src/dashboard/app.py`

## CV Keywords Demonstrated

- Spatiotemporal data analysis
- Exploratory data analysis (EDA)
- Time-series patterns
- Zone/device grouping
- Behavior-based anomaly detection
- SQL, Python, Polars
- Dashboard development (Streamlit)
- Data visualization (Plotly)

## License

This project is for educational purposes. NYC TLC data is public domain.
