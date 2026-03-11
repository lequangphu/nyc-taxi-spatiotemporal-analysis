import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys
import os

# Import from installed package
from nyc_taxi_spatiotemporal_analysis.data.download import download_sample, load_trip_data, get_zone_lookup
from nyc_taxi_spatiotemporal_analysis.data.transform import transform
from nyc_taxi_spatiotemporal_analysis.eda.temporal import (
    trips_by_hour,
    trips_by_day_of_week,
    trips_by_month,
    hourly_patterns_weekday_vs_weekend,
    get_temporal_summary,
)
from nyc_taxi_spatiotemporal_analysis.eda.spatial import (
    top_pickup_zones,
    top_dropoff_zones,
    zone_pair_flows,
    get_spatial_summary,
    zone_heatmap_data,
)
from nyc_taxi_spatiotemporal_analysis.eda.stats import get_basic_stats, get_payment_stats
from nyc_taxi_spatiotemporal_analysis.zones.grouper import (
    add_region_columns,
    trips_by_region,
    region_to_region_flows,
)
from nyc_taxi_spatiotemporal_analysis.anomaly.detector import detect_all_anomalies


# Data directory - use /mount/data on Streamlit Cloud, fallback to local "data"
if Path("/mount/data").exists():
    DATA_DIR = Path("/mount/data")
else:
    DATA_DIR = Path("data")

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data(year: int = 2024, month: int = 1, max_rows: int = 30000) -> pl.DataFrame:
    """Load and transform taxi trip data.

    Downloads and filters actual NYC TLC data to reduce size.
    """
    try:
        # Download with target sample size
        filepath = download_sample(year, month, DATA_DIR, target_rows=max_rows)
        df = load_trip_data(filepath)
        df = transform(df)
        df = add_region_columns(df)
        return df
    except Exception as e:
        import traceback
        st.error(f"Error loading data: {str(e)}")
        st.info("Using realistic sample data instead...")
        # Try to use realistic sample
        try:
            from nyc_taxi_spatiotemporal_analysis.data.download import create_realistic_sample
            filepath = create_realistic_sample(DATA_DIR, year, month, max_rows)
            df = load_trip_data(filepath)
            df = transform(df)
            df = add_region_columns(df)
            return df
        except Exception as e2:
            st.error(f"Could not load sample: {str(e2)}")
            # Return minimal DataFrame to prevent crashes
            return pl.DataFrame()


def plot_trips_by_hour(df: pl.DataFrame):
    """Plot trips by hour of day."""
    if df.is_empty():
        st.warning("No data available")
        return
    data = trips_by_hour(df).to_pandas()
    fig = px.bar(
        data,
        x="pickup_hour",
        y="trip_count",
        title="Trips by Hour of Day",
        labels={"pickup_hour": "Hour", "trip_count": "Trip Count"},
        color="trip_count",
        color_continuous_scale="Viridis",
    )
    fig.update_layout(xaxis=dict(tickmode="linear", tick0=0, dtick=1))
    st.plotly_chart(fig, width="stretch")


def plot_trips_by_day(df: pl.DataFrame):
    """Plot trips by day of week."""
    if df.is_empty():
        st.warning("No data available")
        return
    data = trips_by_day_of_week(df).to_pandas()
    fig = px.bar(
        data,
        x="day_name",
        y="trip_count",
        title="Trips by Day of Week",
        labels={"day_name": "Day", "trip_count": "Trip Count"},
        color="trip_count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        xaxis={
            "categoryorder": "array",
            "categoryarray": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
        }
    )
    st.plotly_chart(fig, width="stretch")


def plot_regional_flows(df: pl.DataFrame):
    """Plot regional flow heatmap."""
    if df.is_empty():
        st.warning("No data available")
        return
    data = region_to_region_flows(df).to_pandas()

    fig = px.density_heatmap(
        data,
        x="pickup_region",
        y="dropoff_region",
        z="trip_count",
        title="Trip Flows Between Regions",
        labels={
            "pickup_region": "Pickup Region",
            "dropoff_region": "Dropoff Region",
            "trip_count": "Trips",
        },
        color_continuous_scale="YlOrRd",
    )
    st.plotly_chart(fig, width="stretch")


def plot_trips_by_region(df: pl.DataFrame):
    """Plot trips by region."""
    if df.is_empty():
        st.warning("No data available")
        return
    data = trips_by_region(df).to_pandas()
    fig = px.pie(
        data,
        values="trip_count",
        names="pickup_region",
        title="Trips by Pickup Region",
        hole=0.4,
    )
    st.plotly_chart(fig, width="stretch")


def run_app():
    """Run the main dashboard application."""
    import streamlit as st

    st.title("🚕 NYC Taxi Spatiotemporal Analysis")
    st.markdown("""
    Exploratory data analysis, zone grouping, and anomaly detection on NYC Yellow Taxi trip data.
    This project demonstrates skills in spatiotemporal data analysis relevant to device telemetry and IoT data.
    """)

    with st.sidebar:
        st.header("Data Selection")
        year = st.selectbox("Year", [2024, 2023, 2022], index=0)
        month = st.selectbox("Month", list(range(1, 13)), index=0)
        st.markdown("---")
        st.markdown("### About")
        st.markdown("""
        This dashboard analyzes NYC Yellow Taxi trip records to demonstrate:
        - **Spatiotemporal EDA**: Understanding patterns in time and space
        - **Zone Grouping**: Grouping 265 taxi zones into logical regions
        - **Anomaly Detection**: Identifying unusual trip patterns
        """)

    # Show loading message
    with st.spinner("Loading taxi data..."):
        df = load_data(year, month)

    # Check if data loaded successfully
    if df.is_empty():
        st.error("Unable to load data. Please try again or select a different time period.")
        return

    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "📊 Overview",
            "⏰ Temporal Analysis",
            "🗺️ Spatial Analysis",
            "🏙️ Zone Grouping",
            "⚠️ Anomaly Detection",
        ]
    )

    with tab1:
        st.header("Dataset Overview")

        stats = get_basic_stats(df)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Trips", f"{stats['total_trips']:,}")
        col2.metric("Date Range", f"{stats['date_range']['start'].strftime('%Y-%m')}")
        col3.metric("Avg Fare", f"${stats['fare']['mean']:.2f}")
        col4.metric("Avg Distance", f"{stats['distance']['mean']:.2f} mi")

        st.subheader("Sample Data")
        st.dataframe(df.head(100), width="stretch")

        st.subheader("Basic Statistics")
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Fare Statistics**")
            st.dataframe(
                {
                    "Metric": ["Mean", "Median", "Std", "Min", "Max"],
                    "Value": [
                        f"${stats['fare']['mean']:.2f}",
                        f"${stats['fare']['median']:.2f}",
                        f"${stats['fare']['std']:.2f}",
                        f"${stats['fare']['min']:.2f}",
                        f"${stats['fare']['max']:.2f}",
                    ],
                },
                hide_index=True,
            )
        with col2:
            st.write("**Trip Distance Statistics**")
            st.dataframe(
                {
                    "Metric": ["Mean", "Median", "Std", "Min", "Max"],
                    "Value": [
                        f"{stats['distance']['mean']:.2f} mi",
                        f"{stats['distance']['median']:.2f} mi",
                        f"{stats['distance']['std']:.2f} mi",
                        f"{stats['distance']['min']:.2f} mi",
                        f"{stats['distance']['max']:.2f} mi",
                    ],
                },
                hide_index=True,
            )

    with tab2:
        st.header("Temporal Analysis")

        col1, col2 = st.columns(2)
        with col1:
            plot_trips_by_hour(df)
        with col2:
            plot_trips_by_day(df)

        st.subheader("Key Temporal Insights")
        temporal_stats = get_temporal_summary(df)
        col1, col2, col3 = st.columns(3)
        col1.metric("Peak Hour", f"{temporal_stats['peak_hour']}:00")
        col2.metric(
            "Avg Trip Duration", f"{temporal_stats['avg_trip_duration_min']:.1f} min"
        )
        col3.metric("Avg Fare", f"${temporal_stats['avg_fare']:.2f}")

        st.subheader("Trips by Month")
        monthly_data = trips_by_month(df).to_pandas()
        fig = px.bar(
            monthly_data,
            x="pickup_month",
            y="trip_count",
            title="Trips by Month",
            labels={"pickup_month": "Month", "trip_count": "Trip Count"},
            color="trip_count",
            color_continuous_scale="Plasma",
        )
        st.plotly_chart(fig, width="stretch")

    with tab3:
        st.header("Spatial Analysis")

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Top 10 Pickup Zones")
            pickup_data = top_pickup_zones(df, 10).to_pandas()
            fig = px.bar(
                pickup_data,
                x="PULocationID",
                y="trip_count",
                title="Top 10 Pickup Zones",
                labels={"PULocationID": "Zone ID", "trip_count": "Trip Count"},
                color="trip_count",
                color_continuous_scale="Greens",
            )
            st.plotly_chart(fig, width="stretch")
        with col2:
            st.subheader("Top 10 Dropoff Zones")
            dropoff_data = top_dropoff_zones(df, 10).to_pandas()
            fig = px.bar(
                dropoff_data,
                x="DOLocationID",
                y="trip_count",
                title="Top 10 Dropoff Zones",
                labels={"DOLocationID": "Zone ID", "trip_count": "Trip Count"},
                color="trip_count",
                color_continuous_scale="Oranges",
            )
            st.plotly_chart(fig, width="stretch")

        st.subheader("Top Zone Flows")
        flow_data = zone_pair_flows(df, 15).to_pandas()
        st.dataframe(
            flow_data.rename(
                columns={
                    "PULocationID": "Pickup Zone",
                    "DOLocationID": "Dropoff Zone",
                    "trip_count": "Trips",
                    "avg_fare": "Avg Fare",
                    "avg_distance": "Avg Distance",
                }
            ),
            width="stretch",
        )

    with tab4:
        st.header("Zone Grouping Analysis")
        st.markdown(
            "Grouping 265 taxi zones into 6 logical regions for behavior-based analysis."
        )

        col1, col2 = st.columns(2)
        with col1:
            plot_trips_by_region(df)
        with col2:
            st.subheader("Regional Summary")
            region_data = trips_by_region(df).to_pandas()
            st.dataframe(
                region_data.rename(
                    columns={
                        "pickup_region": "Region",
                        "trip_count": "Trips",
                        "total_fare": "Total Fare",
                        "avg_fare": "Avg Fare",
                        "avg_distance": "Avg Distance",
                    }
                ),
                width="stretch",
            )

        st.subheader("Regional Flows")
        plot_regional_flows(df)

    with tab5:
        st.header("Anomaly Detection")
        st.markdown(
            "Behavior-based anomaly detection to identify unusual trip patterns."
        )

        anomalies, summary = detect_all_anomalies(df)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Anomalies", f"{summary['total_anomalies']:,}")
        col2.metric("Anomaly Rate", f"{summary['anomaly_rate']:.2f}%")

        st.subheader("Anomaly Breakdown")
        anomaly_data = {
            "Type": [
                "Speed Anomalies",
                "Fare Outliers",
                "Short Distance/Long Duration",
                "Long Distance/Short Duration",
                "Late Night High Fare",
            ],
            "Count": [
                summary.get("speed_anomalies", 0),
                summary.get("fare_anomalies", 0),
                summary.get("short_long_anomalies", 0),
                summary.get("long_short_anomalies", 0),
                summary.get("late_night_high_fare", 0),
            ],
        }
        st.dataframe(anomaly_data, width="stretch")

        st.subheader("Anomalous Trips Sample")
        if len(anomalies) > 0:
            st.dataframe(
                anomalies.select(
                    [
                        "tpep_pickup_datetime",
                        "tpep_dropoff_datetime",
                        "PULocationID",
                        "DOLocationID",
                        "fare_amount",
                        "trip_distance",
                        "trip_duration_minutes",
                        "trip_speed_mph",
                    ]
                ).head(50),
                width="stretch",
            )
        else:
            st.info("No anomalies detected with current thresholds.")
