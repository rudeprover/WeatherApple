import os
import time
import pandas as pd
import folium
from streamlit_folium import st_folium
import streamlit as st
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="Real-Time India Weather Pipeline",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (CSS Injection for a clean visual look)
st.markdown("""
<style>
    .metric-card {
        background-color: #f8fafc;
        border-radius: 8px;
        padding: 15px;
        border-left: 5px solid #3b82f6;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .status-badge {
        background-color: #dcfce7;
        color: #15803d;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.85em;
        font-weight: 600;
    }
    .airflow-tag {
        background-color: #eff6ff;
        color: #1d4ed8;
        padding: 3px 8px;
        border-radius: 12px;
        font-size: 0.85em;
        font-weight: 500;
        margin-left: 10px;
    }
</style>
""", unsafe_allow_html=True)

# 5-minute Auto-Refresh logic
# Embed an HTML meta tag or custom script to refresh the page every 300 seconds
st.markdown('<meta http-equiv="refresh" content="300">', unsafe_allow_html=True)

# Path to the shared CSV file
CSV_FILE_PATH = os.environ.get("WEATHER_CSV_PATH", "data/weather_data.csv")

# Color mapping helper for temperature visualization
def get_temp_color(temp):
    if temp < 15:
        return 'lightblue'
    elif temp < 25:
        return 'green'
    elif temp < 32:
        return 'orange'
    else:
        return 'red'

# Helper to load weather data
@st.cache_data(ttl=60) # Cache for 60 seconds to avoid hitting lock excessively
def load_weather_data(file_path):
    if not os.path.exists(file_path):
        # Fallback to mock initial data if file doesn't exist yet (before DAG's first run)
        fallback_data = pd.DataFrame([
            {"city": "New Delhi", "latitude": 28.6139, "longitude": 77.2090, "temp_c": 31.5, "humidity": 65, "wind_kph": 12.0, "condition": "Partly Cloudy", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Mumbai", "latitude": 19.0760, "longitude": 72.8777, "temp_c": 29.2, "humidity": 80, "wind_kph": 15.4, "condition": "Light Drizzle", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Bengaluru", "latitude": 12.9716, "longitude": 77.5946, "temp_c": 24.5, "humidity": 70, "wind_kph": 18.0, "condition": "Overcast", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Kolkata", "latitude": 22.5726, "longitude": 88.3639, "temp_c": 30.1, "humidity": 78, "wind_kph": 9.2, "condition": "Mist", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Chennai", "latitude": 13.0827, "longitude": 80.2707, "temp_c": 33.4, "humidity": 72, "wind_kph": 14.0, "condition": "Clear Sky", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Hyderabad", "latitude": 17.3850, "longitude": 78.4867, "temp_c": 28.0, "humidity": 62, "wind_kph": 11.2, "condition": "Scattered Clouds", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Srinagar", "latitude": 34.0837, "longitude": 74.7973, "temp_c": 16.8, "humidity": 55, "wind_kph": 5.8, "condition": "Clear Sky", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Jaipur", "latitude": 26.9124, "longitude": 75.7873, "temp_c": 34.2, "humidity": 45, "wind_kph": 10.5, "condition": "Sunny", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            {"city": "Guwahati", "latitude": 26.1445, "longitude": 91.7362, "temp_c": 27.5, "humidity": 82, "wind_kph": 8.0, "condition": "Heavy Rain", "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        ])
        return fallback_data
    
    try:
        df = pd.read_csv(file_path)
        # Parse timestamp column if exists
        if 'updated_at' in df.columns:
            df['updated_at_dt'] = pd.to_datetime(df['updated_at'])
            # Get latest records for each unique city to represent real-time dashboard view
            latest_records = df.sort_values('updated_at_dt').groupby('city').last().reset_index()
            return latest_records
        return df
    except Exception as e:
        st.error(f"Error loading CSV data: {e}")
        return pd.DataFrame()

# Main Header Section
st.title("⚡ Real-Time India Weather Monitoring System")
st.markdown("---")

df_weather = load_weather_data(CSV_FILE_PATH)

if df_weather.empty:
    st.warning("No weather records found! Please ensure Apache Airflow DAGs have run first.")
else:
    # Sidebar
    st.sidebar.header("🎯 System Settings")
    st.sidebar.markdown(f"**Data Pipeline Source:** Airflow Scheduler")
    st.sidebar.markdown(f"**Storage:** Shared CSV (`{CSV_FILE_PATH}`)")
    st.sidebar.markdown("**Update Interaval:** Run every 5 minutes")
    
    # Get last update timestamp
    try:
        last_update_str = df_weather['updated_at'].iloc[0]
        # Format for display
        last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S")
        display_timestamp = last_update.strftime("%b %d, %Y - %H:%M:%S")
    except Exception:
        display_timestamp = datetime.now().strftime("%b %d, %Y - %H:%M:%S (Live simulated)")

    st.sidebar.info(f"Last Pipeline Sync:\n**{display_timestamp}**")
    
    # Add manual refresh button in sidebar just in case
    if st.sidebar.button("🔄 Trigger Manual Refresh"):
        st.cache_data.clear()
        st.rerun()

    # Layout: Metrics + Map
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("📊 Key Weather Indicators")
        
        # Calculate summary numbers
        avg_temp = df_weather['temp_c'].mean()
        max_temp_row = df_weather.loc[df_weather['temp_c'].idxmax()]
        min_temp_row = df_weather.loc[df_weather['temp_c'].idxmin()]
        avg_humidity = df_weather['humidity'].mean()
        
        # Display elegant Key KPI cards
        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 12px; border-left-color: #3b82f6;">
            <p style="margin: 0; color: #64748b; font-size: 0.9em; font-weight: 500;">Average Region Temp</p>
            <h2 style="margin: 3px 0 0 0; color: #1e293b; font-size: 1.8em;">{avg_temp:.1f}°C</h2>
            <span class="status-badge">🔄 Live Feed</span>
            <span class="airflow-tag">Orchestrated</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 12px; border-left-color: #ef4444;">
            <p style="margin: 0; color: #64748b; font-size: 0.9em; font-weight: 500;">Hottest City</p>
            <h2 style="margin: 3px 0 0 0; color: #1e293b; font-size: 1.8em;">{max_temp_row['city']}</h2>
            <p style="margin: 3px 0 0 0; color: #ef4444; font-weight: 600; font-size: 1.1em;">{max_temp_row['temp_c']}°C</p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 12px; border-left-color: #06b6d4;">
            <p style="margin: 0; color: #64748b; font-size: 0.9em; font-weight: 500;">Coolest City</p>
            <h2 style="margin: 3px 0 0 0; color: #1e293b; font-size: 1.8em;">{min_temp_row['city']}</h2>
            <p style="margin: 3px 0 0 0; color: #06b6d4; font-weight: 600; font-size: 1.1em;">{min_temp_row['temp_c']}°C</p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(f"""
        <div class="metric-card" style="margin-bottom: 12px; border-left-color: #10b981;">
            <p style="margin: 0; color: #64748b; font-size: 0.9em; font-weight: 500;">Average humidity</p>
            <h2 style="margin: 3px 0 0 0; color: #1e293b; font-size: 1.8em;">{avg_humidity:.1f}%</h2>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.subheader("🗺️ Live Weather Map (India)")
        
        # Center map on India coordinates
        india_map_center = [22.9734, 78.6568]
        m = folium.Map(location=india_map_center, zoom_start=5, control_scale=True)
        
        # Add city weather markers
        for idx, row in df_weather.iterrows():
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; min-width: 160px; line-height: 1.4;">
                <h4 style="margin: 0 0 5px 0; color: #2c3e50;">{row['city']}</h4>
                <b>Temperature:</b> {row['temp_c']}°C<br/>
                <b>Humidity:</b> {row['humidity']}%<br/>
                <b>Wind Speed:</b> {row['wind_kph']} kph<br/>
                <b>Condition:</b> {row['condition']}<br/>
                <hr style="margin: 5px 0; border: 0; border-top: 1px solid #ccc;" />
                <span style="font-size: 0.8em; color: #7f8c8d;">Updated: {row['updated_at']}</span>
            </div>
            """
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{row['city']}: {row['temp_c']}°C",
                icon=folium.Icon(color=get_temp_color(row['temp_c']), icon='info', prefix='fa')
            ).add_to(m)
            
        # Display folium map in Streamlit
        st_folium(m, width="100%", height=480)

    # Detailed Weather Table Section
    st.markdown("---")
    st.subheader("📋 Latest Weather Datasets (Extracted via Apache Airflow)")
    
    # Render interactive DataFrame
    st.dataframe(
        df_weather[['city', 'temp_c', 'humidity', 'wind_kph', 'condition', 'updated_at']],
        column_config={
            "city": "City Name",
            "temp_c": "Temperature (°C)",
            "humidity": "Humidity (%)",
            "wind_kph": "Wind Speed (kph)",
            "condition": "Skies/Condition",
            "updated_at": "Extracted Timestamp"
        },
        use_container_width=True,
        hide_index=True
    )
    
    # Download Button
    csv = df_weather.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download Weather CSV (Raw Dataset)",
        data=csv,
        file_name="weather_pipeline_output.csv",
        mime="text/csv",
    )
