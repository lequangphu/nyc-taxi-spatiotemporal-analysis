import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from src.data.download import download_sample, load_trip_data, get_zone_lookup
from src.data.transform import transform
from src.eda.temporal import (
    trips_by_hour,
    trips_by_day_of_week,
    trips_by_month,
    hourly_patterns_weekday_vs_weekend,
    get_temporal_summary,
)
from src.eda.spatial import (
    top_pickup_zones,
    top_dropoff_zones,
    zone_pair_flows,
    get_spatial_summary,
    zone_heatmap_data,
)
from src.eda.stats import get_basic_stats, get_payment_stats
from src.zones.grouper import (
    add_region_columns,
    trips_by_region,
    region_to_region_flows,
)
from src.anomaly.detector import detect_all_anomalies


DATA_DIR = Path(__file__).parent.parent.parent / "data"


@st.cache_data
def load_data(year: int = 2024, month: int = 1) -> pl.DataFrame:
    """Load and transform taxi trip data."""
    filepath = download_sample(year, month, DATA_DIR)
    df = load_trip_data(filepath)
    df = transform(df)
    df = add_region_columns(df)
    return df


def plot_trips_by_hour(df: pl.DataFrame):
    """Plot trips by hour of day."""
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
    st.plotly_chart(fig, use_container_width=True)


def plot_trips_by_day(df: pl.DataFrame):
    """Plot trips by day of week."""
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
    st.plotly_chart(fig, use_container_width=True)


def plot_regional_flows(df: pl.DataFrame):
    """Plot regional flow heatmap."""
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
    st.plotly_chart(fig, use_container_width=True)


def plot_trips_by_region(df: pl.DataFrame):
    """Plot trips by region."""
    data = trips_by_region(df).to_pandas()
    fig = px.pie(
        data,
        values="trip_count",
        names="pickup_region",
        title="Trips by Pickup Region",
        hole=0.4,
    )
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.set_page_config(
        page_title="NYC Taxi Spatiotemporal Analysis", page_icon="🚕", layout="wide"
    )

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

    df = load_data(year, month)

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
        st.dataframe(df.head(100), use_container_width=True)

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
        st.plotly_chart(fig, use_container_width=True)

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
            st.plotly_chart(fig, use_container_width=True)
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
            st.plotly_chart(fig, use_container_width=True)

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
            use_container_width=True,
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
                use_container_width=True,
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
        st.dataframe(anomaly_data, use_container_width=True)

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
                use_container_width=True,
            )
        else:
            st.info("No anomalies detected with current thresholds.")


if __name__ == "__main__":
    main()
