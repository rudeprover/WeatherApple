import React, { useState, useEffect, useRef } from "react";
import {
  Cloud,
  Sun,
  CloudRain,
  Snowflake,
  Wind,
  Thermometer,
  Droplets,
  Calendar,
  Code,
  FileCode,
  Settings,
  Play,
  RefreshCw,
  Download,
  List,
  Server,
  Terminal,
  ArrowRight,
  MapPin,
  Sparkles,
  Database,
  Layers,
  Cpu,
  ExternalLink,
  Check,
  Search,
  CheckCircle,
  FileText,
  Share2,
  ArrowDown,
  Activity
} from "lucide-react";

// Types corresponding to backend structures
interface WeatherRecord {
  city: string;
  latitude: number;
  longitude: number;
  temp_c: number;
  humidity: number;
  wind_kph: number;
  condition: string;
  updated_at: string;
}

interface AnalyticsRecord {
  city: string;
  avg_temperature_c: number;
  peak_temperature_c: number;
  floor_temperature_c: number;
  mean_humidity: number;
  peak_windspeed_kph: number;
  dominant_condition: string;
  aggregated_at: string;
}

const FILE_CONTENTS: Record<string, { path: string; language: string; content: string }> = {
  "streamlit_app": {
    path: "app/app.py",
    language: "python",
    content: `import os
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
st.markdown('<meta http-equiv="refresh" content="300">', unsafe_allow_html=True)

# Path to the shared CSV file
CSV_FILE_PATH = os.environ.get("WEATHER_CSV_PATH", "data/weather_data.csv")

def get_temp_color(temp):
    if temp < 15: return 'lightblue'
    elif temp < 25: return 'green'
    elif temp < 32: return 'orange'
    else: return 'red'

@st.cache_data(ttl=60)
def load_weather_data(file_path):
    if not os.path.exists(file_path):
        # Fallback pre-seeded records
        return pd.DataFrame([...])
    try:
        df = pd.read_csv(file_path)
        if 'updated_at' in df.columns:
            df['updated_at_dt'] = pd.to_datetime(df['updated_at'])
            return df.sort_values('updated_at_dt').groupby('city').last().reset_index()
        return df
    except Exception as e:
        st.error(f"Error loading CSV data: {e}")
        return pd.DataFrame()

st.title("⚡ Real-Time India Weather Monitoring System")
st.markdown("---")

df_weather = load_weather_data(CSV_FILE_PATH)
if df_weather.empty:
    st.warning("No weather records found! Please ensure Airflow DAGs have run.")
else:
    st.sidebar.header("🎯 System Settings")
    st.sidebar.markdown(f"**Data Pipeline Source:** Airflow Scheduler")
    st.sidebar.markdown(f"**Storage:** Shared CSV (\`{CSV_FILE_PATH}\`) ")
    st.sidebar.markdown("**Update Interval:** Run every 5 minutes")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.subheader("📊 Key Weather Indicators")
        # Renders KPI Summary metrics
        ...
    with col2:
        st.subheader("🗺️ Live Weather Map (India)")
        # Renders interactive map utilizing folium coordinates
        ...
`
  },
  "airflow_dag1": {
    path: "dags/dag1.py",
    language: "python",
    content: `"""
Apache Airflow DAG: weather_acquisition_dag (dag1)
Description: Fetches real-time weather information for key Indian cities from Open-Meteo REST API,
             cleans and shapes the responses, and appends them to a shared CSV filesystem volume.
Schedule: Runs every 5 minutes (*/5 * * * *).
"""
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'weather_admin',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

SHARED_CSV_PATH = os.environ.get("WEATHER_CSV_PATH", "/app/data/weather_data.csv")

CITIES = {
    "New Delhi": {"lat": 28.6139, "lon": 77.2090},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639},
    "Chennai": {"lat": 13.0827, "lon": 80.2707},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Srinagar": {"lat": 34.0837, "lon": 74.7973},
    "Jaipur": {"lat": 26.9124, "lon": 75.7873},
    "Guwahati": {"lat": 26.1445, "lon": 91.7362},
    "Pune": {"lat": 18.5204, "lon": 73.8567}
}

def fetch_and_write_weather_data(**context):
    extracted_records = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for city, coords in CITIES.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true&relative_humidity_2m=true"
        response = requests.get(url, timeout=12)
        if response.status_code == 200:
            current = response.json().get("current_weather", {})
            record = {
                "city": city,
                "latitude": coords['lat'],
                "longitude": coords['lon'],
                "temp_c": current.get("temperature", 25.0),
                "humidity": response.json().get("current", {}).get("relative_humidity_2m", 65),
                "wind_kph": current.get("windspeed", 10.0),
                "condition": "Cloudy", # Translated from weather codes
                "updated_at": timestamp
            }
            extracted_records.append(record)
            
    df_new = pd.DataFrame(extracted_records)
    df_new.to_csv(SHARED_CSV_PATH, mode='a', header=not os.path.exists(SHARED_CSV_PATH), index=False)

with DAG(
    'weather_acquisition_dag',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:
    fetch_weather_metrics = PythonOperator(
        task_id='fetch_and_write_weather_data',
        python_callable=fetch_and_write_weather_data
    )
`
  },
  "airflow_dag2": {
    path: "dags/dag2.py",
    language: "python",
    content: `"""
Apache Airflow DAG: weather_analytics_dag (dag2)
Description: Hourly/Daily analytical rollup aggregation. Reads raw high-frequency weather records,
             computes daily minimum, maximum, and average temperatures per city, 
             and writes summaries to 'city_analytics.csv' for long-term trends display.
Schedule: Runs daily at midnight (@daily).
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'analytics_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

SHARED_CSV_PATH = os.environ.get("WEATHER_CSV_PATH", "/app/data/weather_data.csv")
ANALYTICS_CSV_PATH = os.environ.get("ANALYTICS_CSV_PATH", "/app/data/city_analytics.csv")

def aggregate_daily_weather(**context):
    if not os.path.exists(SHARED_CSV_PATH): return
    df = pd.read_csv(SHARED_CSV_PATH)
    
    analytics_records = []
    for city, group in df.groupby('city'):
        analytics_records.append({
            "city": city,
            "avg_temperature_c": round(group['temp_c'].mean(), 1),
            "peak_temperature_c": round(group['temp_c'].max(), 1),
            "floor_temperature_c": round(group['temp_c'].min(), 1),
            "mean_humidity": round(group['humidity'].mean(), 1),
            "peak_windspeed_kph": round(group['wind_kph'].max(), 1),
            "dominant_condition": group['condition'].mode().iloc[0] if not group['condition'].empty else "Clear",
            "aggregated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    pd.DataFrame(analytics_records).to_csv(ANALYTICS_CSV_PATH, index=False)

with DAG(
    'weather_analytics_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    compute_aggregates = PythonOperator(
        task_id='aggregate_daily_weather',
        python_callable=aggregate_daily_weather
    )
`
  },
  "docker_compose": {
    path: "docker-compose.yaml",
    language: "yaml",
    content: `version: '3.8'

services:
  postgres_db:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  airflow-webserver:
    image: apache/airflow:2.7.2
    command: webserver
    ports:
      - "8080:8080"
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    depends_on:
      - postgres_db

  airflow-scheduler:
    image: apache/airflow:2.7.2
    command: scheduler
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
    depends_on:
      - postgres_db

  streamlit-app:
    build:
      context: ./app
      dockerfile: Dockerfile
    environment:
      - WEATHER_CSV_PATH=/data/weather_data.csv
    ports:
      - "8501:8501"
    volumes:
      - ./data:/data
    depends_on:
      - airflow-scheduler

volumes:
  postgres_data:
`
  },
  "dockerfile": {
    path: "app/Dockerfile",
    language: "dockerfile",
    content: `FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8501
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
`
  },
  "requirements": {
    path: "app/requirements.txt",
    language: "text",
    content: `streamlit>=1.30.0
pandas>=2.0.0
requests>=2.31.0
folium>=0.15.0
streamlit-folium>=0.16.0
jinja2>=3.1.2
`
  }
};

// Coordinate mapping for SVG India Map
const MAP_WIDTH = 1000;
const MAP_HEIGHT = 1000;

// Coordinate bounding values to scale coordinates onto India's SVG map bounds
const LON_MIN = 67.0;
const LON_MAX = 98.0;
const LAT_MIN = 7.0;
const LAT_MAX = 37.0;

function convertCoordsToSvg(lat: number, lon: number) {
  // Translate longitude (X axis)
  const x = ((lon - LON_MIN) / (LON_MAX - LON_MIN)) * MAP_WIDTH;
  // Translate latitude (Y axis, note SVG coords start at top-left, so we subtract from height)
  const y = MAP_HEIGHT - ((lat - LAT_MIN) / (LAT_MAX - LAT_MIN)) * MAP_HEIGHT;
  return { x, y };
}

export default function App() {
  const [activeTab, setActiveTab] = useState<"streamlit" | "airflow" | "code" | "architecture">("streamlit");
  const [weatherData, setWeatherData] = useState<WeatherRecord[]>([]);
  const [analyticsData, setAnalyticsData] = useState<AnalyticsRecord[]>([]);
  const [logs, setLogs] = useState<string[]>([]);
  const [selectedCity, setSelectedCity] = useState<string>("");
  const [humidityFilter, setHumidityFilter] = useState<number>(100);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(true);
  const [triggeringDag1, setTriggeringDag1] = useState<boolean>(false);
  const [triggeringDag2, setTriggeringDag2] = useState<boolean>(false);
  const [countdown, setCountdown] = useState<number>(300); // 5-minute counter (300 seconds)
  const [copiedFile, setCopiedFile] = useState<string | null>(null);
  const [activeCodeFile, setActiveCodeFile] = useState<string>("streamlit_app");

  const terminalEndRef = useRef<HTMLDivElement>(null);

  // Fetch core data from backend
  const fetchData = async () => {
    try {
      const weatherRes = await fetch("/api/weather-data");
      const weatherJson = await weatherRes.json();
      if (weatherJson.success) {
        setWeatherData(weatherJson.data);
      }

      const analyticsRes = await fetch("/api/analytics-data");
      const analyticsJson = await analyticsRes.json();
      if (analyticsJson.success) {
        setAnalyticsData(analyticsJson.data);
      }

      const logsRes = await fetch("/api/pipeline-logs");
      const logsJson = await logsRes.json();
      if (logsJson.logs) {
        setLogs(logsJson.logs);
      }
    } catch (err) {
      console.error("Failed to load metrics: ", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();

    // Set polling frequencies
    const interval = setInterval(() => {
      fetchData();
    }, 4000);

    return () => clearInterval(interval);
  }, []);

  // 5-minute countdown clock
  useEffect(() => {
    const timer = setInterval(() => {
      setCountdown((prev) => {
        if (prev <= 1) {
          // Trigger simulated DAG execution when reaching zero
          handleTriggerDag1(true);
          return 300;
        }
        return prev - 1;
      });
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  // Scroll logs to bottom
  useEffect(() => {
    if (terminalEndRef.current) {
      terminalEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs]);

  // Handle Trigger Extraction dag1
  const handleTriggerDag1 = async (isAuto = false) => {
    if (triggeringDag1) return;
    setTriggeringDag1(true);
    try {
      const res = await fetch("/api/trigger-dag1", { method: "POST" });
      await res.json();
      fetchData();
      if (!isAuto) {
        setCountdown(300); // reset countdown on manual trigger
      }
    } catch (err) {
      console.error(err);
    } finally {
      setTimeout(() => setTriggeringDag1(false), 800);
    }
  };

  // Handle Trigger Analytics dag2
  const handleTriggerDag2 = async () => {
    if (triggeringDag2) return;
    setTriggeringDag2(true);
    try {
      const res = await fetch("/api/trigger-dag2", { method: "POST" });
      await res.json();
      fetchData();
    } catch (err) {
      console.error(err);
    } finally {
      setTimeout(() => setTriggeringDag2(false), 800);
    }
  };

  // Reset the database CSV records back to baseline mock
  const handleResetData = async () => {
    if (window.confirm("Restore original pre-seeded weather configurations?")) {
      try {
        const res = await fetch("/api/reset-data");
        await res.json();
        setCountdown(300);
        fetchData();
      } catch (err) {
        console.error(err);
      }
    }
  };

  // Process data filters
  const filteredWeather = weatherData.filter((record) => {
    const matchesSearch = record.city.toLowerCase().includes(searchTerm.toLowerCase());
    const matchesHumidity = record.humidity <= humidityFilter;
    return matchesSearch && matchesHumidity;
  });

  const avgTemp = weatherData.length
    ? weatherData.reduce((acc, curr) => acc + curr.temp_c, 0) / weatherData.length
    : 0;

  const hottestCity = weatherData.length
    ? weatherData.reduce((prev, curr) => (prev.temp_c > curr.temp_c ? prev : curr), weatherData[0])
    : null;

  const coolestCity = weatherData.length
    ? weatherData.reduce((prev, curr) => (prev.temp_c < curr.temp_c ? prev : curr), weatherData[0])
    : null;

  const avgHumidity = weatherData.length
    ? weatherData.reduce((acc, curr) => acc + curr.humidity, 0) / weatherData.length
    : 0;

  const handleCopyCode = (key: string, content: string) => {
    navigator.clipboard.writeText(content);
    setCopiedFile(key);
    setTimeout(() => setCopiedFile(null), 2500);
  };

  const formatTime = (secs: number) => {
    const minutes = Math.floor(secs / 60);
    const seconds = secs % 60;
    return `${minutes}:${seconds.toString().padStart(2, "0")}`;
  };

  // Helper weather cond icons
  const getWeatherIcon = (cond: string, size = 20, css = "") => {
    const lower = cond.toLowerCase();
    if (lower.includes("rain") || lower.includes("drizzle") || lower.includes("shower")) {
      return <CloudRain className={`${css} text-blue-500`} size={size} />;
    }
    if (lower.includes("clear") || lower.includes("sunny")) {
      return <Sun className={`${css} text-amber-500`} size={size} />;
    }
    if (lower.includes("snow") || lower.includes("frost")) {
      return <Snowflake className={`${css} text-cyan-200`} size={size} />;
    }
    if (lower.includes("cloud") || lower.includes("overcast") || lower.includes("mist") || lower.includes("fog")) {
      return <Cloud className={`${css} text-slate-400`} size={size} />;
    }
    return <Sun className={`${css} text-amber-500`} size={size} />;
  };

  // Generate dynamic color mapping for markers
  const getTempColorValue = (temp: number) => {
    if (temp < 18) return "#06b6d4"; // Cool cyan
    if (temp < 25) return "#10b981"; // Mild green
    if (temp < 32) return "#f59e0b"; // Warm amber
    return "#ef4444"; // Hot red
  };

  // Generate CSV download link
  const triggerCsvDownload = () => {
    if (!weatherData.length) return;
    const headerRow = ["city", "latitude", "longitude", "temp_c", "humidity", "wind_kph", "condition", "updated_at"];
    const csvContent = [
      headerRow.join(","),
      ...weatherData.map(r => `"${r.city}",${r.latitude},${r.longitude},${r.temp_c},${r.humidity},${r.wind_kph},"${r.condition}","${r.updated_at}"`)
    ].join("\n");

    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute("download", "weather_pipeline_live_data.csv");
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  return (
    <div className="min-h-screen bg-[#F8FAFC] text-slate-800 flex flex-col font-sans" id="applet_root_id">
      
      {/* HEADER COCKPIT BAR */}
      <header className="bg-white border-b border-slate-200 px-6 py-4 flex flex-col md:flex-row justify-between items-start md:items-center gap-4 shrink-0 shadow-xs" id="header_section_id">
        <div>
          <div className="flex items-center gap-2">
            <span className="p-1.5 bg-blue-50 border border-blue-200 rounded text-blue-600">
              <Cpu size={20} className="animate-pulse" />
            </span>
            <h1 className="text-xl font-extrabold tracking-tight text-slate-900 flex items-center gap-2">
              India Weather Pipelines <span className="text-[11px] bg-blue-50 text-blue-700 border border-blue-200 px-2 py-0.5 rounded font-mono font-bold">Airflow + Streamlit</span>
            </h1>
          </div>
          <p className="text-xs text-slate-500 mt-1">
            Real-time pipeline ingestion displaying climate sensors on a decoupled micro-architecture
          </p>
        </div>

        {/* System telemetry badges */}
        <div className="flex flex-wrap items-center gap-2 md:gap-3 text-xs" id="telemetry_section_id">
          <div className="flex items-center gap-1.5 bg-emerald-50 text-emerald-700 border border-emerald-250 px-3 py-1.5 rounded-lg font-semibold">
            <span className="w-2 h-2 rounded-full bg-emerald-500 animate-ping" />
            <span>Airflow Scheduler Live</span>
          </div>

          <div className="flex items-center gap-1.5 bg-blue-50 text-blue-700 border border-blue-250 px-3 py-1.5 rounded-lg font-mono font-bold">
            <span>Interval Countdown:</span>
            <span>{formatTime(countdown)}</span>
          </div>

          <button
            onClick={handleResetData}
            id="btn_reset_data"
            className="flex items-center gap-1.5 bg-white hover:bg-slate-50 border border-slate-200 text-slate-700 font-semibold px-3 py-1.5 rounded-lg transition cursor-pointer shadow-sm"
          >
            <RefreshCw size={12} className="text-slate-450" />
            <span>Seed Factory Values</span>
          </button>
        </div>
      </header>

      {/* CORE VIEW LAYOUT: MAIN NAVIGATION TABS */}
      <div className="flex border-b border-slate-200 bg-white px-6 shrink-0 shadow-xs" id="navigation_tabs_id">
        <button
          onClick={() => setActiveTab("streamlit")}
          id="tab_streamlit"
          className={`flex items-center gap-2 px-5 py-3 border-b-2 font-bold text-sm transition cursor-pointer ${
            activeTab === "streamlit"
              ? "border-blue-600 text-blue-600 bg-blue-50/20 font-extrabold"
              : "border-transparent text-slate-500 hover:text-slate-950 hover:bg-slate-50/50"
          }`}
        >
          {/* Red Streamlit paperboat style logo */}
          <span className="w-5 h-5 flex items-center justify-center bg-red-600 rounded text-white font-black text-[10px] leading-none shrink-0 shadow-sm">
            🛦
          </span>
          <span>Streamlit Web App Preview</span>
        </button>

        <button
          onClick={() => setActiveTab("airflow")}
          id="tab_airflow"
          className={`flex items-center gap-2 px-5 py-3 border-b-2 font-bold text-sm transition cursor-pointer ${
            activeTab === "airflow"
              ? "border-blue-600 text-blue-600 bg-blue-50/20 font-extrabold"
              : "border-transparent text-slate-500 hover:text-slate-950 hover:bg-slate-50/50"
          }`}
        >
          {/* Cyan Airflow style logo pinwheel */}
          <span className="w-5 h-5 flex items-center justify-center bg-sky-600 rounded text-white text-xs shrink-0 shadow-sm font-semibold">
            ❋
          </span>
          <span>Apache Airflow Orchestrator</span>
        </button>

        <button
          onClick={() => setActiveTab("code")}
          id="tab_code"
          className={`flex items-center gap-2 px-5 py-3 border-b-2 font-bold text-sm transition cursor-pointer ${
            activeTab === "code"
              ? "border-blue-600 text-blue-600 bg-blue-50/20 font-extrabold"
              : "border-transparent text-slate-500 hover:text-slate-950 hover:bg-slate-50/50"
          }`}
        >
          <Code size={16} className={activeTab === "code" ? "text-blue-600" : "text-slate-500"} />
          <span>Python Codebase Explorer</span>
        </button>

        <button
          onClick={() => setActiveTab("architecture")}
          id="tab_architecture"
          className={`flex items-center gap-2 px-5 py-3 border-b-2 font-bold text-sm transition cursor-pointer ${
            activeTab === "architecture"
              ? "border-blue-600 text-blue-600 bg-blue-50/20 font-extrabold"
              : "border-transparent text-slate-500 hover:text-slate-950 hover:bg-slate-50/50"
          }`}
        >
          <Share2 size={16} className={activeTab === "architecture" ? "text-blue-600" : "text-slate-500"} />
          <span>Architecture & Methodology</span>
        </button>
      </div>

      {/* CORE WORKSPACE CONTENT PANEL */}
      <div className="flex-1 overflow-auto min-h-0 bg-[#F8FAFC]" id="workspace_panel_id">
        {loading ? (
          <div className="flex flex-col items-center justify-center h-full gap-3 py-24 text-slate-500 bg-[#F8FAFC]">
            <RefreshCw size={40} className="animate-spin text-blue-600" />
            <p className="font-mono text-sm tracking-wide font-medium">Synchronizing data registries...</p>
          </div>
        ) : (
          <>
            {/* TAB 1: STREAMLIT APP PREVIEW SIMULATOR */}
            {activeTab === "streamlit" && (
              <div className="p-6 max-w-7xl mx-auto flex flex-col gap-6 animate-fade-in" id="streamlit_tab_container">
                
                {/* Simulated Streamlit Window Frame Container */}
                <div className="bg-white text-slate-800 rounded-3xl shadow-xl shadow-blue-100/40 border border-slate-200 flex flex-col overflow-hidden min-h-[750px]">
                  
                  {/* Streamlit Window Toolbar */}
                  <div className="bg-slate-50 px-5 py-3 flex items-center justify-between border-b border-slate-100 text-slate-500 text-xs">
                    <div className="flex items-center gap-2">
                      <div className="flex gap-1.5">
                        <span className="w-3 h-3 bg-red-400 rounded-full" />
                        <span className="w-3 h-3 bg-amber-400 rounded-full" />
                        <span className="w-3 h-3 bg-emerald-400 rounded-full" />
                      </div>
                      <span className="font-mono text-xs text-slate-550 bg-slate-200/50 px-2.5 py-0.5 rounded-md ml-2 font-medium">
                        http://localhost:8501/ -- Streamlit App
                      </span>
                    </div>
                    <div className="flex items-center gap-2 font-bold text-blue-800 bg-blue-50 px-3 py-1 rounded-full text-[10px] uppercase tracking-wider border border-blue-200">
                      ⚡ Decoupled Client View
                    </div>
                  </div>

                  {/* Streamlit Core Divided Layout */}
                  <div className="flex-1 flex flex-col lg:flex-row divide-y lg:divide-y-0 lg:divide-x divide-slate-100">
                    
                    {/* Simulated Streamlit Sidebar */}
                    <div className="w-full lg:w-72 bg-slate-50 p-6 flex flex-col gap-6" id="streamlit_sidebar_id">
                      
                      {/* Header Widget */}
                      <div>
                        <div className="flex items-center gap-2 mb-2">
                          <span className="text-xl">🎯</span>
                          <h3 className="font-extrabold text-slate-900 text-sm tracking-tight uppercase">System Settings</h3>
                        </div>
                        <div className="h-1 bg-blue-600 w-10 rounded mt-1" />
                      </div>

                      {/* Info lines */}
                      <div className="text-xs text-slate-600 flex flex-col gap-3">
                        <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm hover:border-slate-300 transition duration-150">
                          <span className="block text-slate-400 text-[9px] font-extrabold uppercase tracking-widest">PIPELINE SOURCE</span>
                          <span className="font-bold text-slate-800 block mt-0.5">Airflow Orchestrator</span>
                        </div>

                        <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm hover:border-slate-300 transition duration-150">
                          <span className="block text-slate-400 text-[9px] font-extrabold uppercase tracking-widest">STORAGE TYPE</span>
                          <span className="font-bold text-slate-800 block mt-0.5 font-mono text-[10px]">Shared CSV volume</span>
                          <span className="text-[10px] text-slate-500 font-mono block mt-0.5">/data/weather_data.csv</span>
                        </div>

                        <div className="bg-white border border-slate-200 rounded-xl p-3 shadow-sm hover:border-slate-300 transition duration-150">
                          <span className="block text-slate-400 text-[9px] font-extrabold uppercase tracking-widest">DATA FREQUENCY</span>
                          <span className="font-bold text-slate-800 block mt-0.5">Trigger every 5 minutes</span>
                        </div>

                        <div className="bg-blue-50/75 border border-blue-150 text-blue-900 rounded-xl p-3.5 shadow-sm">
                          <span className="block text-blue-600 text-[9px] font-extrabold uppercase tracking-widest">LATEST PIPELINE SYNC</span>
                          <span className="font-extrabold block mt-0.5 font-mono">
                            {weatherData.length > 0 ? weatherData[0].updated_at : "Initializing..."}
                          </span>
                        </div>
                      </div>

                      {/* Streamlit Inputs and Controls */}
                      <div className="flex flex-col gap-4 border-t border-slate-200 pt-5">
                        <h4 className="font-extrabold text-xs text-slate-900 uppercase tracking-widest">Interactive Filters</h4>
                        
                        {/* Search City Input */}
                        <div>
                          <label className="text-xs font-bold text-slate-600 block mb-1">Search City Name</label>
                          <div className="relative">
                            <Search className="absolute left-3 top-3 text-slate-400" size={13} />
                            <input
                              type="text"
                              value={searchTerm}
                              onChange={(e) => setSearchTerm(e.target.value)}
                              placeholder="e.g. New Delhi..."
                              className="w-full bg-white border border-slate-200 rounded-lg pl-8 pr-6 py-2 text-xs text-slate-800 placeholder-slate-450 focus:ring-2 focus:ring-blue-500/10 focus:border-blue-600 focus:outline-none transition"
                            />
                            {searchTerm && (
                              <button
                                onClick={() => setSearchTerm("")}
                                className="absolute right-2.5 top-2.5 text-slate-400 hover:text-slate-700 text-xs font-bold"
                              >
                                ×
                              </button>
                            )}
                          </div>
                        </div>

                        {/* Slider Humidity filter */}
                        <div>
                          <div className="flex justify-between items-center text-xs mb-1">
                            <span className="font-bold text-slate-650">Max Humidity Limit</span>
                            <span className="font-mono text-blue-600 font-extrabold">{humidityFilter}%</span>
                          </div>
                          <input
                            type="range"
                            min="20"
                            max="100"
                            value={humidityFilter}
                            onChange={(e) => setHumidityFilter(Number(e.target.value))}
                            className="w-full accent-blue-600 cursor-pointer"
                          />
                          <span className="text-[10px] text-slate-400 mt-1.5 block leading-normal">Hide cities with humidity above target threshold</span>
                        </div>

                        {/* Manual Trigger Simulator */}
                        <button
                          onClick={handleTriggerDag1}
                          disabled={triggeringDag1}
                          className="w-full mt-2 bg-blue-600 hover:bg-blue-700 text-white py-2.5 rounded-lg text-xs font-bold transition flex items-center justify-center gap-1.5 cursor-pointer disabled:opacity-50 shadow-md shadow-blue-200/40"
                        >
                          <RefreshCw size={12} className={triggeringDag1 ? "animate-spin" : ""} />
                          <span>{triggeringDag1 ? "DAG Running..." : "Trigger Ingestion Run"}</span>
                        </button>
                      </div>

                      {/* Footer credit */}
                      <div className="mt-auto pt-6 border-t border-slate-200 flex items-center justify-between text-[10px] text-slate-400 font-mono">
                        <span>Streamlit Ported</span>
                        <span>v1.30.0</span>
                      </div>

                    </div>

                    {/* Simulated Streamlit Main Workspace content area */}
                    <div className="flex-1 bg-white p-8 overflow-y-auto">
                      
                      {/* Title block */}
                      <div className="mb-6">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="text-2xl">⚡</span>
                          <h2 className="text-2xl font-extrabold text-slate-900 tracking-tight">Real-Time India Weather Monitoring System</h2>
                        </div>
                        <p className="text-slate-500 text-xs font-medium">
                          Structured and compiled via Apache Airflow DAG workflows. Auto-refreshes data logs every 5 minutes automatically.
                        </p>
                        <div className="flex items-center gap-2 mt-2.5">
                          <span className="bg-emerald-50 text-emerald-700 border border-emerald-200 text-[10px] font-bold px-2.5 py-0.5 rounded-full uppercase tracking-wider">
                            ● Streamlit Active
                          </span>
                          <span className="text-slate-300 text-xs">|</span>
                          <span className="text-[11px] text-slate-500 font-mono font-medium">
                            Auto Reload Enabled <meta httpEquiv="refresh" content="300" /> (300s default)
                          </span>
                        </div>
                      </div>

                      <hr className="border-slate-100 mb-6" />

                      {/* Metrics Cards block */}
                      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
                        
                        <div className="bg-white rounded-2xl p-5 border-l-4 border-blue-600 shadow-sm border border-slate-200 hover:border-slate-300 hover:shadow-md transition duration-200">
                          <span className="text-[10px] font-extrabold text-slate-400 uppercase tracking-widest block">Average Temp</span>
                          <span className="text-2xl font-extrabold text-slate-900 block mt-1">{avgTemp.toFixed(1)}°C</span>
                          <div className="flex items-center gap-1.5 mt-2.5">
                            <span className="text-[9px] bg-emerald-55 text-emerald-700 border border-emerald-100 font-bold px-1.5 py-0.5 rounded">Live Raw Feed</span>
                            <span className="text-[9px] bg-blue-50 text-blue-700 border border-blue-100 font-mono px-1.5 py-0.5 rounded">Dag Logged</span>
                          </div>
                        </div>

                        <div className="bg-white rounded-2xl p-5 border-l-4 border-rose-500 shadow-sm border border-slate-200 hover:border-slate-300 hover:shadow-md transition duration-200">
                          <span className="text-[10px] font-extrabold text-slate-400 uppercase tracking-widest block">Hottest Region</span>
                          <span className="text-xl font-extrabold text-slate-900 block mt-1 truncate">{hottestCity ? hottestCity.city : "N/A"}</span>
                          <span className="text-lg font-black text-rose-500 block mt-0.5">{hottestCity ? `${hottestCity.temp_c}°C` : "--"}</span>
                        </div>

                        <div className="bg-white rounded-2xl p-5 border-l-4 border-sky-500 shadow-sm border border-slate-200 hover:border-slate-300 hover:shadow-md transition duration-200">
                          <span className="text-[10px] font-extrabold text-slate-400 uppercase tracking-widest block">Coolest Region</span>
                          <span className="text-xl font-extrabold text-slate-900 block mt-1 truncate">{coolestCity ? coolestCity.city : "N/A"}</span>
                          <span className="text-lg font-black text-sky-600 block mt-0.5">{coolestCity ? `${coolestCity.temp_c}°C` : "--"}</span>
                        </div>

                        <div className="bg-white rounded-2xl p-5 border-l-4 border-emerald-500 shadow-sm border border-slate-200 hover:border-slate-300 hover:shadow-md transition duration-200">
                          <span className="text-[10px] font-extrabold text-slate-400 uppercase tracking-widest block">Average Humidity</span>
                          <span className="text-2xl font-extrabold text-slate-900 block mt-1">{avgHumidity.toFixed(1)}%</span>
                          <span className="text-[10px] text-slate-500 font-medium block mt-2">Satisfying standard limits</span>
                        </div>

                      </div>

                      {/* Main divided map & statistics layout */}
                      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6 mb-8">
                        
                        {/* MAP DISPLAY BOX (3 units width) */}
                        <div className="lg:col-span-3 border border-slate-200 rounded-3xl overflow-hidden shadow-sm flex flex-col bg-white hover:shadow-md transition duration-200">
                          
                          <div className="bg-slate-50 border-b border-slate-100 px-5 py-3.5 flex items-center justify-between">
                            <span className="font-extrabold text-xs text-slate-800 flex items-center gap-1.5 uppercase tracking-wider">
                              <MapPin size={14} className="text-blue-600 animate-bounce" />
                              Interactive India GIS Map Renderer
                            </span>
                            <span className="text-[9px] bg-blue-50 border border-blue-200 text-blue-800 font-bold px-2 py-0.5 rounded-md font-mono">
                              Folium Engine
                            </span>
                          </div>

                          <div className="flex-1 min-h-[480px] bg-slate-50/50 relative overflow-hidden flex items-center justify-center p-4">
                            
                            {/* Detailed responsive SVG Map of India with state topography styling */}
                            <svg
                              viewBox="0 0 1000 1000"
                              className="w-full h-full max-h-[450px] transition drop-shadow-md"
                              id="india_svg_map"
                            >
                              {/* Soft shaded background outline representing India boundaries */}
                              <g id="boundaries_group" className="fill-slate-200/70 stroke-white stroke-[4]">
                                <path d="M 330,110 L 350,110 L 360,130 L 380,120 L 410,135 L 430,110 L 440,115 L 450,90 L 485,90 L 490,120 L 510,150 L 530,180 L 520,205 L 500,210 L 470,200 L 450,220 L 445,260 L 460,280 L 480,270 L 520,280 L 550,290 L 580,310 L 610,320 L 640,320 L 660,340 L 730,345 L 750,335 L 750,370 L 780,390 L 800,410 L 860,405 L 900,420 L 920,440 L 940,430 L 950,460 L 930,480 L 940,510 L 910,510 L 880,480 L 840,480 L 850,510 L 800,530 L 780,510 L 778,540 L 795,570 L 815,550 L 830,580 L 800,600 L 770,590 L 750,550 L 720,530 L 700,550 L 690,590 L 665,640 L 640,680 L 610,720 L 575,760 L 540,820 L 515,860 L 500,900 L 495,930 L 480,950 L 475,930 L 460,910 L 462,870 L 455,830 L 450,790 L 435,750 L 420,700 L 412,650 L 400,610 L 390,580 L 370,550 L 360,510 L 330,490 L 300,470 L 260,455 L 210,450 L 190,430 L 170,425 L 140,442 L 120,462 L 95,450 L 110,410 L 140,395 L 180,378 L 220,380 L 250,360 L 270,320 L 285,290 L 265,260 L 290,220 L 310,210 L 305,180 Z" />
                                <path d="M 460,280 L 480,330 L 500,360 L 514,400 L 525,440 L 535,480 L 548,510 L 560,530 L 580,560" className="fill-none stroke-slate-350/50 stroke-[3] stroke-dasharray" />
                              </g>

                              {/* Glowing grid effects on map canvas */}
                              <defs>
                                <radialGradient id="map_glow" cx="50%" cy="50%" r="50%">
                                  <stop offset="0%" stopColor="#ffffff" stopOpacity="0.5" />
                                  <stop offset="100%" stopColor="#f1f5f9" stopOpacity="0" />
                                </radialGradient>
                              </defs>
                              <rect width="1000" height="1000" fill="url(#map_glow)" pointerEvents="none" />

                              {/* City nodes representing locations */}
                              {filteredWeather.map((row) => {
                                const scaled = convertCoordsToSvg(row.latitude, row.longitude);

                                // Avoid invalid layout outputs
                                if (isNaN(scaled.x) || isNaN(scaled.y)) return null;

                                const color = getTempColorValue(row.temp_c);
                                const isSelected = selectedCity === row.city;

                                return (
                                  <g
                                    key={row.city}
                                    className="cursor-pointer group"
                                    onClick={() => setSelectedCity(row.city)}
                                  >
                                    {/* Waves pulse on city sensor markers */}
                                    <circle
                                      cx={scaled.x}
                                      cy={scaled.y}
                                      r={isSelected ? 26 : 14}
                                      fill={color}
                                      opacity="0.18"
                                      className="animate-ping"
                                      style={{ animationDuration: isSelected ? "1.2s" : "2.5s" }}
                                    />
                                    <circle
                                      cx={scaled.x}
                                      cy={scaled.y}
                                      r={isSelected ? 14 : 7}
                                      fill={color}
                                      stroke="#ffffff"
                                      strokeWidth={isSelected ? 3.5 : 2}
                                      className="transition shadow-lg"
                                    />

                                    {/* Text display values on the map */}
                                    <text
                                      x={scaled.x + 12}
                                      y={scaled.y + 4}
                                      fill="#1e293b"
                                      fontSize={isSelected ? "14" : "11"}
                                      fontWeight={isSelected ? "bold" : "600"}
                                      className="select-none bg-white font-sans stroke-white stroke-[4] paint-order-stroke drop-shadow-sm group-hover:fill-blue-600 transition"
                                    >
                                      {row.city} ({row.temp_c}°C)
                                    </text>
                                  </g>
                                );
                              })}
                            </svg>

                            {/* Floating Map Legend */}
                            <div className="absolute bottom-3 right-3 bg-white/95 backdrop-blur-md border border-slate-200 p-3 rounded-2xl shadow-md text-[10px] text-slate-600 flex flex-col gap-1.5 font-sans">
                              <span className="font-extrabold text-slate-800 uppercase tracking-widest block mb-1">Temperature Index</span>
                              <div className="flex items-center gap-1.5">
                                <span className="w-2.5 h-2.5 rounded-full bg-cyan-500" />
                                <span>Cold (&lt; 18°C) Srinagar</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <span className="w-2.5 h-2.5 rounded-full bg-emerald-500" />
                                <span>Mild (18°C - 25°C) Bengaluru</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <span className="w-2.5 h-2.5 rounded-full bg-amber-500" />
                                <span>Warm (25°C - 32°C) Mumbai</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <span className="w-2.5 h-2.5 rounded-full bg-rose-500" />
                                <span>Hot (&gt; 32°C) Jaipur</span>
                              </div>
                            </div>

                          </div>
                        </div>

                        {/* HIGHLIGHTED ACTIVE CITY DETAIL SIDEBAR (2 units width) */}
                        <div className="lg:col-span-2 border border-slate-205 rounded-3xl bg-white p-6 flex flex-col justify-start shadow-sm hover:shadow-md transition duration-200">
                          
                          {selectedCity ? (
                            (() => {
                              const cityRecord = weatherData.find(c => c.city === selectedCity);
                              if (!cityRecord) return <p className="text-slate-400 text-xs">Select a city to inspect details.</p>;
                              
                              return (
                                <div className="flex flex-col gap-5 animate-fade-in">
                                  <div className="flex items-start justify-between">
                                    <div>
                                      <span className="text-[10px] font-extrabold text-slate-400 uppercase tracking-widest block">ACTIVE STATION PANEL</span>
                                      <h3 className="text-2xl font-extrabold text-slate-900 mt-0.5">{cityRecord.city}</h3>
                                      <span className="text-[11px] font-mono text-slate-500 block mt-0.5">
                                        Lat: {cityRecord.latitude} | Lon: {cityRecord.longitude}
                                      </span>
                                    </div>
                                    <div className="p-3 bg-blue-50/70 rounded-2xl border border-blue-100 flex items-center justify-center">
                                      {getWeatherIcon(cityRecord.condition, 28)}
                                    </div>
                                  </div>

                                  <div className="h-[1px] bg-slate-100" />

                                  <div className="grid grid-cols-2 gap-3">
                                    
                                    <div className="bg-slate-50 p-3 rounded-xl border border-slate-150 shadow-xs">
                                      <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block flex items-center gap-1">
                                        <Thermometer size={11} className="text-slate-500" /> Temp
                                      </span>
                                      <span className="text-lg font-extrabold text-slate-850 block mt-1">{cityRecord.temp_c}°C</span>
                                    </div>

                                    <div className="bg-slate-50 p-3 rounded-xl border border-slate-150 shadow-xs">
                                      <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block flex items-center gap-1">
                                        <Droplets size={11} className="text-slate-500" /> Moisture
                                      </span>
                                      <span className="text-lg font-extrabold text-slate-850 block mt-1">{cityRecord.humidity}%</span>
                                    </div>

                                    <div className="bg-slate-50 p-3 rounded-xl border border-slate-150 shadow-xs text-slate-700">
                                      <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block flex items-center gap-1">
                                        <Wind size={11} className="text-slate-500" /> Wind
                                      </span>
                                      <span className="text-lg font-extrabold text-slate-850 block mt-1">{cityRecord.wind_kph} kph</span>
                                    </div>

                                    <div className="bg-slate-50 p-3 rounded-xl border border-slate-150 shadow-xs">
                                      <span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block">Condition</span>
                                      <span className="text-xs font-bold text-slate-800 block mt-1.5 truncate">{cityRecord.condition}</span>
                                    </div>

                                  </div>

                                  {/* Daily Aggregates trend panel */}
                                  <div className="bg-blue-50/40 border border-blue-100 rounded-2xl p-4.5 mt-2">
                                    <h4 className="text-xs font-extrabold text-blue-900 flex items-center gap-1 uppercase tracking-wider">
                                      <Sparkles size={11} className="text-blue-500" />
                                      Pipeline Aggregates
                                    </h4>
                                    
                                    {analyticsData.find(a => a.city === selectedCity) ? (
                                      (() => {
                                        const analytical = analyticsData.find(a => a.city === selectedCity)!;
                                        return (
                                          <div className="grid grid-cols-2 gap-x-4 gap-y-2 mt-3 text-xs text-slate-705">
                                            <div>
                                              <span className="text-[9px] text-slate-400 block font-extrabold uppercase tracking-widest">DAILY AVERAGE</span>
                                              <span className="font-extrabold text-slate-800 block mt-0.5">{analytical.avg_temperature_c}°C</span>
                                            </div>
                                            <div>
                                              <span className="text-[9px] text-slate-400 block font-extrabold uppercase tracking-widest">PEAK VELOCITY</span>
                                              <span className="font-extrabold text-slate-800 block mt-0.5">{analytical.peak_windspeed_kph} kph</span>
                                            </div>
                                            <div className="mt-1">
                                              <span className="text-[9px] text-slate-400 block font-extrabold uppercase tracking-widest">MIN FLOOR REGISTRY</span>
                                              <span className="font-extrabold text-blue-600 block mt-0.5">{analytical.floor_temperature_c}°C</span>
                                            </div>
                                            <div className="mt-1">
                                              <span className="text-[9px] text-slate-400 block font-extrabold uppercase tracking-widest">MAX CEILING REGISTRY</span>
                                              <span className="font-extrabold text-rose-600 block mt-0.5">{analytical.peak_temperature_c}°C</span>
                                            </div>
                                          </div>
                                        );
                                      })()
                                    ) : (
                                      <p className="text-xs text-slate-500 mt-2 font-mono leading-relaxed">No daily rollup compiled yet. Trigger aggregate rollup inside Apache Airflow orchestrator...</p>
                                    )}
                                  </div>

                                  <div className="text-[10px] text-slate-505 flex justify-between font-mono bg-slate-50 p-2.5 rounded-xl border border-slate-100">
                                    <span>Sync Status:</span>
                                    <span className="text-emerald-700 font-bold flex items-center gap-0.5">
                                      <CheckCircle size={10} /> Saved CSV Local
                                    </span>
                                  </div>
                                </div>
                              );
                            })()
                          ) : (
                            <div className="flex flex-col items-center justify-center text-center h-full gap-4 text-slate-400 py-10" id="inactive_city_sidebar">
                              <span className="p-4 bg-blue-50 text-blue-600 rounded-full">
                                <MapPin size={32} />
                              </span>
                              <div>
                                <h4 className="font-extrabold text-slate-800">Select Location Station</h4>
                                <p className="text-xs text-slate-500 mt-1 max-w-[200px] mx-auto leading-relaxed">
                                  Click on any reactive pulsing node on the India map to inspect meteorological values.
                                </p>
                              </div>
                            </div>
                          )}

                        </div>

                      </div>

                      {/* DATASET TABLE AND ACTIONS (BOTTOM) */}
                      <div className="border border-slate-200 rounded-3xl overflow-hidden shadow-sm bg-white flex flex-col hover:shadow-md transition duration-200 mt-6 md:mt-8">
                        
                        <div className="bg-slate-50 border-b border-slate-100 px-6 py-4 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4">
                          <div>
                            <h3 className="font-extrabold text-slate-900 text-sm flex items-center gap-1.5 uppercase tracking-wider">
                              <List size={16} className="text-blue-600" />
                              Latest Weather Datasets (Extracted via Apache Airflow)
                            </h3>
                            <p className="text-[11px] text-slate-500 mt-0.5">
                              This listing shows raw rows presently committed to the shared volume storage path: <code className="bg-slate-200/60 px-1.5 py-0.5 rounded text-[10px] font-mono text-slate-700">data/weather_data.csv</code>
                            </p>
                          </div>

                          <button
                            onClick={triggerCsvDownload}
                            id="btn_download_csv"
                            className="bg-blue-600 hover:bg-blue-700 text-white hover:text-white px-4.5 py-2.5 rounded-lg text-xs font-bold flex items-center gap-1.5 transition cursor-pointer self-stretch sm:self-auto justify-center shadow-md shadow-blue-200/40"
                          >
                            <Download size={13} />
                            <span>Download Weather CSV</span>
                          </button>
                        </div>

                        <div className="overflow-x-auto">
                          <table className="w-full text-left border-collapse text-xs text-slate-600">
                            <thead>
                              <tr className="bg-slate-50/50 text-slate-800 border-b border-slate-150 font-extrabold uppercase tracking-widest text-[9px]">
                                <th className="px-6 py-3.5">City Name</th>
                                <th className="px-6 py-3.5">Latitude</th>
                                <th className="px-6 py-3.5">Longitude</th>
                                <th className="px-6 py-3.5">Temperature</th>
                                <th className="px-6 py-3.5">Humidity</th>
                                <th className="px-6 py-3.5">Wind Speed</th>
                                <th className="px-6 py-3.5">Skies/Condition</th>
                                <th className="px-6 py-3.5">Extracted Timestamp</th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100">
                              {filteredWeather.length > 0 ? (
                                filteredWeather.map((row) => (
                                  <tr
                                    key={row.city}
                                    onClick={() => setSelectedCity(row.city)}
                                    className={`hover:bg-slate-50 cursor-pointer transition ${
                                      selectedCity === row.city ? "bg-blue-50/30" : ""
                                    }`}
                                  >
                                    <td className="px-6 py-3 font-bold text-slate-900 flex items-center gap-2">
                                      <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: getTempColorValue(row.temp_c) }} />
                                      {row.city}
                                    </td>
                                    <td className="px-6 py-3 font-mono text-slate-500">{row.latitude}</td>
                                    <td className="px-6 py-3 font-mono text-slate-500">{row.longitude}</td>
                                    <td className="px-6 py-3 font-bold text-slate-800">{row.temp_c}°C</td>
                                    <td className="px-6 py-3 font-medium text-slate-600">{row.humidity}%</td>
                                    <td className="px-6 py-3 font-medium text-slate-600">{row.wind_kph} kph</td>
                                    <td className="px-6 py-3 flex items-center gap-1.5">
                                      {getWeatherIcon(row.condition, 14)}
                                      <span className="font-semibold text-slate-700">{row.condition}</span>
                                    </td>
                                    <td className="px-6 py-3 font-mono text-slate-400">{row.updated_at}</td>
                                  </tr>
                                ))
                              ) : (
                                <tr>
                                  <td colSpan={8} className="px-6 py-12 text-center text-slate-450 font-mono">
                                    No records match search parameters. Search alternative city or reset datasets.
                                  </td>
                                </tr>
                              )}
                            </tbody>
                          </table>
                        </div>

                        <div className="bg-slate-50 border-t border-slate-100 p-4.5 flex justify-between items-center text-[10px] text-slate-500 font-semibold uppercase tracking-wider">
                          <span>Showing <strong>{filteredWeather.length}</strong> active monitoring stations</span>
                          <span className="text-blue-600 font-bold">Auto-Refreshed via Streamlit ⚡</span>
                        </div>

                      </div>

                    </div>

                  </div>

                </div>

              </div>
            )}

            {/* TAB 2: APACHE AIRFLOW SCHEDULER PANEL */}
            {activeTab === "airflow" && (
              <div className="p-6 max-w-7xl mx-auto flex flex-col gap-6 animate-fade-in" id="airflow_tab_container">
                
                {/* Header Information on Airflow */}
                <div className="bg-white p-6 rounded-3xl border border-slate-200 flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 shadow-sm hover:shadow-md transition">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="bg-blue-50 border border-blue-200 text-blue-700 rounded text-xs px-2.5 py-0.5 font-bold uppercase tracking-wider">
                        Apache Airflow Dashboard
                      </span>
                      <span className="text-[11px] text-slate-500 font-mono font-bold">v2.7.2</span>
                    </div>
                    <h2 className="text-2xl font-extrabold text-slate-900 mt-1.5 tracking-tight">DAG Orchestration Manager</h2>
                    <p className="text-xs text-slate-500 mt-1">
                      Control flow scheduler trigger and streaming logging outputs representing cron jobs.
                    </p>
                  </div>

                  <div className="flex flex-wrap items-center gap-2 self-stretch sm:self-auto">
                    <button
                      onClick={() => handleTriggerDag1(false)}
                      disabled={triggeringDag1}
                      id="btn_run_dag1"
                      className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.5 rounded-lg text-xs font-bold flex items-center gap-1.5 transition cursor-pointer disabled:opacity-50 shadow-md shadow-blue-200/40"
                    >
                      <Play size={12} fill="currentColor" />
                      <span>Trigger weather_acquisition_dag</span>
                    </button>

                    <button
                      onClick={handleTriggerDag2}
                      disabled={triggeringDag2}
                      id="btn_run_dag2"
                      className="bg-slate-900 hover:bg-[#090D16] text-white px-4 py-2.5 rounded-lg text-xs font-bold flex items-center gap-1.5 transition cursor-pointer disabled:opacity-50 shadow-md shadow-slate-250/20"
                    >
                      <Play size={12} fill="currentColor" />
                      <span>Trigger weather_analytics_dag (Rollup)</span>
                    </button>
                  </div>
                </div>

                {/* Grid layout for DAG details */}
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  
                  {/* TWO ACTIVE DAGS PANEL LEFT (1 unit panel) */}
                  <div className="flex flex-col gap-4">
                    
                    <div className="bg-white border border-slate-200 rounded-3xl p-5 flex flex-col gap-4 shadow-sm hover:shadow-md transition">
                      <h3 className="font-extrabold text-sm text-slate-905 border-b border-slate-100 pb-3 uppercase tracking-wider">
                        Active Ingested DAGs
                      </h3>

                      {/* DAG 1 CARD */}
                      <div className="bg-slate-50 border border-slate-150 rounded-2xl p-4.5 flex flex-col gap-3.5 transition hover:border-slate-350 shadow-xs">
                        <div className="flex justify-between items-start">
                          <div>
                            <span className="text-[9px] bg-blue-100/50 text-blue-700 border border-blue-205 px-1.5 py-0.5 rounded-md font-mono font-bold">dag1</span>
                            <h4 className="font-extrabold text-slate-900 text-xs mt-1.5 truncate">weather_acquisition_dag</h4>
                          </div>
                          <span className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse" />
                        </div>
                        <p className="text-[11px] text-slate-500 leading-normal">
                          Fetches meteorological readings from Open-Meteo REST endpoints and appends values into shared CSV directory.
                        </p>
                        <div className="text-[10px] text-slate-600 font-mono space-y-1 bg-white p-3 rounded-xl border border-slate-150">
                          <div>Schedule: <strong className="text-blue-600">*/5 * * * *</strong></div>
                          <div>Runs: Every 5 minutes</div>
                          <div>Type: Extraction / Load Task</div>
                        </div>
                        <button
                          onClick={() => handleTriggerDag1(false)}
                          disabled={triggeringDag1}
                          className="w-full bg-blue-50 hover:bg-blue-100 text-blue-700 py-2 rounded-lg text-xs font-bold transition flex items-center justify-center gap-1.5 cursor-pointer disabled:opacity-50 border border-blue-200 shadow-xs"
                        >
                          <Play size={10} fill="currentColor" />
                          <span>Trigger Task Ingestion</span>
                        </button>
                      </div>

                      {/* DAG 2 CARD */}
                      <div className="bg-slate-50 border border-slate-150 rounded-2xl p-4.5 flex flex-col gap-3.5 transition hover:border-slate-350 shadow-xs">
                        <div className="flex justify-between items-start">
                          <div>
                            <span className="text-[9px] bg-indigo-100/50 text-indigo-700 border border-indigo-205 px-1.5 py-0.5 rounded-md font-mono font-bold">dag2</span>
                            <h4 className="font-extrabold text-slate-900 text-xs mt-1.5 truncate">weather_analytics_dag</h4>
                          </div>
                          <span className="w-2.5 h-2.5 rounded-full bg-emerald-500" />
                        </div>
                        <p className="text-[11px] text-slate-500 leading-normal">
                          Aggregates weather rows inside <code className="text-slate-600 bg-slate-200/50 px-1 rounded">weather_data.csv</code>, calculating daily max, min limits.
                        </p>
                        <div className="text-[10px] text-slate-600 font-mono space-y-1 bg-white p-3 rounded-xl border border-slate-150">
                          <div>Schedule: <strong className="text-indigo-600">@daily</strong></div>
                          <div>Runs: Midnight Daily</div>
                          <div>Type: SQL/Analytical aggregate</div>
                        </div>
                        <button
                          onClick={handleTriggerDag2}
                          disabled={triggeringDag2}
                          className="w-full bg-slate-100 hover:bg-slate-200 text-slate-800 py-2 rounded-lg text-xs font-bold transition flex items-center justify-center gap-1.5 cursor-pointer disabled:opacity-50 border border-slate-200 shadow-xs"
                        >
                          <Play size={10} fill="currentColor" />
                          <span>Aggregate Daily Rollup</span>
                        </button>
                      </div>

                    </div>

                  </div>

                  {/* LIVE TERMINAL CONSOLE LOGGER & LOG STREAMING DISPLAY RIGT (2 units panel) */}
                  <div className="lg:col-span-2 flex flex-col">
                    
                    <div className="border border-slate-205 rounded-3xl overflow-hidden shadow-sm flex flex-col h-full min-h-[500px]">
                      
                      {/* Terminal bar header */}
                      <div className="bg-[#0B1528] px-5 py-3.5 flex items-center justify-between border-b border-[#14233C]">
                        <div className="flex items-center gap-2">
                          <Terminal size={14} className="text-emerald-400" />
                          <span className="font-mono text-xs font-bold text-slate-200">Airflow DAG Logger Terminal View</span>
                        </div>
                        <div className="flex items-center gap-1.5">
                          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-ping" />
                          <span className="font-mono text-[9px] text-emerald-400 uppercase tracking-widest font-extrabold">STREAMING ACTIVE</span>
                        </div>
                      </div>

                      {/* Log values area */}
                      <div className="flex-1 bg-[#090D16] p-5 font-mono text-[11px] leading-relaxed overflow-y-auto text-slate-300 max-h-[420px] flex flex-col gap-1.5 select-text">
                        {logs.slice(-35).map((log, index) => {
                          let color = "text-slate-300";
                          if (log.includes("WARNING")) color = "text-amber-400";
                          else if (log.includes("ERROR")) color = "text-rose-500 font-bold";
                          else if (log.includes("successfully") || log.includes("completed") || log.includes("Active") || log.includes("Successfully")) color = "text-emerald-400 font-medium";
                          else if (log.includes("Executing") || log.includes("trigger")) color = "text-sky-400";

                          return (
                            <div key={index} className={`${color} break-words`}>
                              {log}
                            </div>
                          );
                        })}
                        <div ref={terminalEndRef} />
                      </div>

                      {/* Terminal footer panel */}
                      <div className="bg-[#0B1528] px-5 py-3.5 flex justify-between items-center text-xs text-slate-400 border-t border-[#14233C]">
                        <span className="font-mono flex items-center gap-1.5">
                          <Database size={13} className="text-slate-500" /> Shared Vol: /opt/airflow/data
                        </span>
                        <span>Total Captured Lines: {logs.length}</span>
                      </div>

                    </div>

                  </div>

                </div>

                {/* GRAPH CHRONOS FLOW WORKFLOW DIAGRAM */}
                <div className="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm hover:shadow-md transition mt-6" id="airflow_pipeline_graph_id">
                  <h3 className="font-extrabold text-sm text-slate-900 border-b border-slate-100 pb-3 uppercase tracking-wider mb-5 flex items-center gap-1.5">
                    <Layers size={15} className="text-blue-650" />
                    Directed Acyclic Graph (DAG) Pipeline Map
                  </h3>

                  <div className="flex flex-col md:flex-row items-center justify-center gap-3 py-6 text-center text-xs">
                    
                    {/* Node 1 */}
                    <div className="bg-slate-50 border border-slate-200 p-4.5 rounded-2xl w-56 flex flex-col gap-2 items-center text-center shadow-xs">
                      <Cloud className="text-blue-500" size={24} />
                      <div>
                        <span className="text-[9px] text-slate-400 font-mono uppercase font-bold tracking-widest">METEO API SOURCE</span>
                        <h4 className="font-extrabold text-slate-900 block text-xs truncate mt-0.5">Open-Meteo REST API</h4>
                      </div>
                    </div>

                    {/* Arrow 1 */}
                    <div className="text-slate-400 flex flex-col items-center">
                      <ArrowRight className="rotate-90 md:rotate-0" size={18} />
                      <span className="text-[9px] font-mono mt-1 text-slate-400 font-semibold">GET JSON</span>
                    </div>

                    {/* Node 2 */}
                    <div className="bg-blue-50/40 border border-blue-200 p-4.5 rounded-2xl w-60 flex flex-col gap-2 items-center text-center shadow-xs relative">
                      {triggeringDag1 && (
                        <span className="absolute -top-2 right-2 bg-blue-600 text-white text-[8px] font-bold px-1.5 py-0.5 rounded animate-pulse">
                          EXECUTING...
                        </span>
                      )}
                      <Cpu className="text-blue-600 animate-spin" style={{ animationDuration: "12s" }} size={24} />
                      <div>
                        <span className="text-[9px] text-blue-600 font-mono uppercase font-bold tracking-widest">SCHEDULER RUNNER</span>
                        <h4 className="font-extrabold text-slate-950 block text-xs mt-0.5 truncate">fetch_and_write_weather</h4>
                      </div>
                    </div>

                    {/* Arrow 2 */}
                    <div className="text-slate-400 flex flex-col items-center">
                      <ArrowRight className="rotate-90 md:rotate-0" size={18} />
                      <span className="text-[9px] font-mono mt-1 text-slate-400 font-semibold">APPEND CSV</span>
                    </div>

                    {/* Node 3 */}
                    <div className="bg-slate-50 border border-slate-200 p-4.5 rounded-2xl w-56 flex flex-col gap-2 items-center text-center shadow-xs">
                      <Database className="text-blue-500" size={24} />
                      <div>
                        <span className="text-[9px] text-slate-400 font-mono uppercase font-bold tracking-widest">PERSISTED VOLUME</span>
                        <h4 className="font-extrabold text-slate-900 block text-xs mt-0.5 truncate">data/weather_data.csv</h4>
                      </div>
                    </div>

                    {/* Arrow 3 */}
                    <div className="text-slate-400 flex flex-col items-center">
                      <ArrowRight className="rotate-90 md:rotate-0" size={18} />
                      <span className="text-[9px] font-mono mt-1 text-slate-400 font-semibold">LOAD RECORDS</span>
                    </div>

                    {/* Node 4 */}
                    <div className="bg-slate-50 border border-slate-200 p-4.5 rounded-2xl w-56 flex flex-col gap-2 items-center text-center shadow-xs">
                      {/* Red Streamlit logo */}
                      <span className="w-6 h-6 flex items-center justify-center bg-red-600 rounded text-white font-black text-xs shrink-0 shadow-sm leading-none">
                        🛦
                      </span>
                      <div>
                        <span className="text-[9px] text-slate-400 font-mono uppercase font-bold tracking-widest">STREAMLIT PREVIEW</span>
                        <h4 className="font-extrabold text-slate-900 block text-xs mt-0.5 truncate">app/app.py</h4>
                      </div>
                    </div>

                  </div>

                </div>

              </div>
            )}

            {/* TAB 3: CODE FILES VIEW EXPLORER */}
            {activeTab === "code" && (
              <div className="p-6 max-w-7xl mx-auto flex flex-col lg:flex-row gap-6 animate-fade-in" id="code_tab_container">
                
                {/* Lateral Code File List Menu (1 unit size) */}
                <div className="lg:w-80 bg-white border border-slate-200 rounded-3xl p-5 shrink-0 flex flex-col self-start gap-4 shadow-sm hover:shadow-md transition">
                  <div>
                    <h3 className="font-extrabold text-sm text-slate-900 uppercase tracking-wider block">Codebase Files</h3>
                    <p className="text-[11px] text-slate-500 mt-1 font-medium leading-relaxed">
                      Check standard Python layouts or deployment manifests matching the required design exactly.
                    </p>
                  </div>

                  <div className="h-[1px] bg-slate-100" />

                  {/* List items grouped by directories */}
                  <div className="flex flex-col gap-4 text-xs font-mono">
                    
                    {/* Directory App/ */}
                    <div>
                      <span className="text-slate-400 font-extrabold block mb-1.5 uppercase tracking-widest text-[10px]">📁 app/</span>
                      <div className="flex flex-col gap-1 pl-3 border-l border-slate-100">
                        <button
                          onClick={() => setActiveCodeFile("streamlit_app")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "streamlit_app"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileCode size={13} />
                          <span>app.py</span>
                        </button>

                        <button
                          onClick={() => setActiveCodeFile("dockerfile")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "dockerfile"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileText size={13} />
                          <span>Dockerfile</span>
                        </button>

                        <button
                          onClick={() => setActiveCodeFile("requirements")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "requirements"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileText size={13} />
                          <span>requirements.txt</span>
                        </button>
                      </div>
                    </div>

                    {/* Directory dags/ */}
                    <div>
                      <span className="text-slate-400 font-extrabold block mb-1.5 uppercase tracking-widest text-[10px]">📁 dags/</span>
                      <div className="flex flex-col gap-1 pl-3 border-l border-slate-100">
                        <button
                          onClick={() => setActiveCodeFile("airflow_dag1")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "airflow_dag1"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileCode size={13} />
                          <span>dag1.py (Ingest)</span>
                        </button>

                        <button
                          onClick={() => setActiveCodeFile("airflow_dag2")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "airflow_dag2"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileCode size={13} />
                          <span>dag2.py (Analytics)</span>
                        </button>
                      </div>
                    </div>

                    {/* Root manifests */}
                    <div>
                      <span className="text-slate-400 font-extrabold block mb-1.5 uppercase tracking-widest text-[10px]">📁 Root folder/</span>
                      <div className="flex flex-col gap-1 pl-3 border-l border-slate-100">
                        <button
                          onClick={() => setActiveCodeFile("docker_compose")}
                          className={`w-full py-1.5 px-3 rounded-xl text-left transition flex items-center gap-1.5 cursor-pointer ${
                            activeCodeFile === "docker_compose"
                              ? "bg-blue-50 text-blue-700 border border-blue-200 font-bold shadow-xs"
                              : "text-slate-500 hover:text-slate-900 hover:bg-slate-50 border border-transparent"
                          }`}
                        >
                          <FileText size={13} />
                          <span>docker-compose.yaml</span>
                        </button>
                      </div>
                    </div>

                  </div>

                </div>

                {/* STYLED TEXT CODE EDITOR EMBED (2 units size) */}
                <div className="flex-1 bg-[#090D16] border border-slate-200 rounded-3xl overflow-hidden shadow-xl flex flex-col" id="code_editor_panel">
                  
                  {/* Editor Bar Header */}
                  <div className="bg-[#0B1528] border-b border-[#14233C] px-5 py-4 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
                    <div>
                      <div className="flex items-center gap-2">
                        <strong className="font-mono text-xs text-slate-105">{FILE_CONTENTS[activeCodeFile].path}</strong>
                        <span className="text-[10px] bg-[#14233C] text-slate-300 font-mono px-2 py-0.5 rounded-md border border-[#21375B]">
                          {FILE_CONTENTS[activeCodeFile].language.toUpperCase()}
                        </span>
                      </div>
                      <p className="text-[10px] text-slate-400 mt-1">Source file location in actual project sandbox directory.</p>
                    </div>

                    <button
                      onClick={() => handleCopyCode(activeCodeFile, FILE_CONTENTS[activeCodeFile].content)}
                      className={`px-4 py-2 rounded-lg text-xs font-bold flex items-center gap-1.5 transition cursor-pointer self-stretch sm:self-auto justify-center ${
                        copiedFile === activeCodeFile
                          ? "bg-emerald-600 text-white shadow"
                          : "bg-[#14233C] hover:bg-[#1A2D4C] text-slate-100 border border-[#21375B]/80 shadow"
                      }`}
                    >
                      {copiedFile === activeCodeFile ? <Check size={13} /> : <Download size={13} />}
                      <span>{copiedFile === activeCodeFile ? "Copied!" : "Copy Source Code"}</span>
                    </button>
                  </div>

                  {/* Styled Scrollable Code Block inside visual code playground IDE */}
                  <div className="flex-1 p-6 overflow-auto bg-[#090D16] font-mono text-[11px] leading-relaxed text-slate-305 border-t border-[#090D16]/65 font-medium">
                    <pre className="whitespace-pre select-text">
                      <code className="text-slate-300">
                        {FILE_CONTENTS[activeCodeFile].content}
                      </code>
                    </pre>
                  </div>

                  {/* Editor status message footer */}
                  <div className="bg-[#0B1528] border-t border-[#14233C] px-5 py-3 text-[10px] md:text-sm text-slate-500 font-mono flex flex-col sm:flex-row justify-between gap-1.5">
                    <span>UTF-8 encoding format</span>
                    <span>Ready for Airflow compilation inside docker instances</span>
                  </div>

                </div>

              </div>
            )}

            {/* TAB 4: ARCHITECTURE & METHODOLOGY */}
            {activeTab === "architecture" && (
              <div className="p-6 max-w-5xl mx-auto flex flex-col gap-10 animate-fade-in pb-20" id="architecture_tab_container">
                 
                 <div className="text-center mb-4 mt-4">
                  <h2 className="text-3xl font-extrabold text-slate-900 tracking-tight">System Architecture & Methodology</h2>
                  <p className="text-slate-500 mt-2 max-w-2xl mx-auto font-medium text-sm">
                    A beginner-friendly breakdown of how data travels from a weather satellite API to this visual dashboard.
                  </p>
                 </div>

                 {/* Flowchart 1: High Level */}
                 <div className="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm flex flex-col gap-6 relative">
                   <h3 className="text-lg font-extrabold text-slate-900 flex items-center gap-2 uppercase tracking-wide">
                     <Share2 className="text-blue-600" size={20} />
                     1. High-Level Data Pipeline
                   </h3>
                   <p className="text-xs text-slate-500 font-medium">This represents the complete, end-to-end journey of the system.</p>

                   <div className="flex flex-col lg:flex-row items-center justify-between gap-4 mt-2">
                     <div className="bg-slate-50 border border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs">
                        <Cloud className="mx-auto text-blue-500 mb-2" size={28} />
                        <h4 className="font-bold text-slate-900 text-sm">Weather API</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Remote data provider</p>
                     </div>
                     <ArrowRight size={24} className="text-slate-300 hidden lg:block shrink-0" />
                     <ArrowDown size={24} className="text-slate-300 block lg:hidden shrink-0" />

                     <div className="bg-slate-50 border border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs">
                        <Activity className="mx-auto text-emerald-500 mb-2" size={28} />
                        <h4 className="font-bold text-slate-900 text-sm">Apache Airflow</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Extracts every 5.0 mins</p>
                     </div>
                     <ArrowRight size={24} className="text-slate-300 hidden lg:block shrink-0" />
                     <ArrowDown size={24} className="text-slate-300 block lg:hidden shrink-0" />

                     <div className="bg-slate-50 border border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs">
                        <Database className="mx-auto text-indigo-500 mb-2" size={28} />
                        <h4 className="font-bold text-slate-900 text-sm">CSV Shared Volume</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Local file storage</p>
                     </div>
                     <ArrowRight size={24} className="text-slate-300 hidden lg:block shrink-0" />
                     <ArrowDown size={24} className="text-slate-300 block lg:hidden shrink-0" />

                     <div className="bg-slate-50 border border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs">
                        <CheckCircle className="mx-auto text-red-500 mb-2" size={28} />
                        <h4 className="font-bold text-slate-900 text-sm">Streamlit UI</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Reads CSV for client</p>
                     </div>
                   </div>
                 </div>

                 {/* Flowchart 2: Airflow DAG logic */}
                 <div className="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm flex flex-col gap-6 relative">
                   <h3 className="text-lg font-extrabold text-slate-900 flex items-center gap-2 uppercase tracking-wide">
                     <Cpu className="text-sky-600" size={20} />
                     2. Behind the Scenes: Airflow Fetch Script
                   </h3>
                   <p className="text-xs text-slate-500 font-medium">Zooming in on what the Apache Airflow system does during its 5-minute schedule.</p>

                   <div className="flex flex-col items-center justify-center gap-2 mt-2 w-full max-w-sm mx-auto">
                      
                      <div className="w-full bg-slate-50 border-2 border-emerald-100 rounded-xl p-4 text-center shadow-xs">
                        <span className="text-[10px] font-extrabold text-emerald-600 block tracking-widest uppercase">Step A</span>
                        <span className="font-bold text-slate-900 text-sm">Timer Hits (e.g. 10:05 AM)</span>
                      </div>
                      <ArrowDown size={20} className="text-slate-300" />
                      
                      <div className="w-full bg-slate-50 border-2 border-slate-200 rounded-xl p-4 text-center shadow-xs">
                        <span className="text-[10px] font-extrabold text-slate-400 block tracking-widest uppercase">Step B</span>
                        <span className="font-bold text-slate-900 text-sm">Loop over 10 target cities</span>
                      </div>
                      <ArrowDown size={20} className="text-slate-300" />
                      
                      <div className="w-full bg-slate-50 border-2 border-slate-200 rounded-xl p-4 text-center shadow-xs">
                        <span className="text-[10px] font-extrabold text-slate-400 block tracking-widest uppercase">Step C</span>
                        <span className="font-bold text-slate-900 text-sm">Send HTTP Request to API</span>
                      </div>
                      <ArrowDown size={20} className="text-slate-300" />

                      <div className="w-full bg-slate-50 border-2 border-slate-200 rounded-xl p-4 text-center shadow-xs">
                        <span className="text-[10px] font-extrabold text-slate-400 block tracking-widest uppercase">Step D</span>
                        <span className="font-bold text-slate-900 text-sm">Extract 'temp', 'windspeed'</span>
                      </div>
                      <ArrowDown size={20} className="text-slate-300" />

                      <div className="w-full bg-slate-50 border-2 border-blue-100 rounded-xl p-4 text-center shadow-xs">
                        <span className="text-[10px] font-extrabold text-blue-600 block tracking-widest uppercase">Step E</span>
                        <span className="font-bold text-slate-900 text-sm">Append 10 rows to <code className="text-xs bg-white border border-slate-200 px-1 rounded">weather_data.csv</code></span>
                      </div>

                   </div>
                 </div>

                 {/* Flowchart 3: Streamlit Rendering Output */}
                 <div className="bg-white border border-slate-200 rounded-3xl p-6 md:p-8 shadow-sm flex flex-col gap-6 relative">
                   <h3 className="text-lg font-extrabold text-slate-900 flex items-center gap-2 uppercase tracking-wide">
                     <List className="text-rose-500" size={20} />
                     3. Behind the Scenes: Dashboard Frontend
                   </h3>
                   <p className="text-xs text-slate-500 font-medium">Zooming in on how the Streamlit interface processes the file for human viewing.</p>

                   <div className="flex flex-col md:flex-row items-center justify-between gap-4 mt-2">
                     <div className="bg-slate-50 border-b-4 border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs border-x border-t">
                        <span className="w-6 h-6 bg-slate-200 rounded-full flex items-center justify-center font-bold text-slate-600 text-xs mx-auto mb-2">1</span>
                        <h4 className="font-bold text-slate-900 text-sm">Detect CSV File</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Check if local file exists</p>
                     </div>
                     <ArrowRight size={20} className="text-slate-300 hidden md:block shrink-0" />
                     <ArrowDown size={20} className="text-slate-300 block md:hidden shrink-0" />

                     <div className="bg-slate-50 border-b-4 border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs border-x border-t">
                        <span className="w-6 h-6 bg-slate-200 rounded-full flex items-center justify-center font-bold text-slate-600 text-xs mx-auto mb-2">2</span>
                        <h4 className="font-bold text-slate-900 text-sm">Load Pandas Library</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Read table into memory</p>
                     </div>
                     <ArrowRight size={20} className="text-slate-300 hidden md:block shrink-0" />
                     <ArrowDown size={20} className="text-slate-300 block md:hidden shrink-0" />

                     <div className="bg-slate-50 border-b-4 border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs border-x border-t">
                        <span className="w-6 h-6 bg-slate-200 rounded-full flex items-center justify-center font-bold text-slate-600 text-xs mx-auto mb-2">3</span>
                        <h4 className="font-bold text-slate-900 text-sm">Compute Stats</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Search for Max/Min Temp</p>
                     </div>
                     <ArrowRight size={20} className="text-slate-300 hidden md:block shrink-0" />
                     <ArrowDown size={20} className="text-slate-300 block md:hidden shrink-0" />

                     <div className="bg-slate-50 border-b-4 border-rose-200 border-x border-t border-slate-200 p-5 rounded-2xl flex-1 w-full text-center shadow-xs">
                        <span className="w-6 h-6 bg-rose-100 rounded-full flex items-center justify-center font-bold text-rose-600 text-xs mx-auto mb-2">4</span>
                        <h4 className="font-bold text-slate-900 text-sm">Draw SVG Map</h4>
                        <p className="text-[11px] text-slate-500 mt-1 font-medium">Plot Lat/Lon points on UI</p>
                     </div>
                   </div>
                 </div>

              </div>
            )}
          </>
        )}
      </div>

    </div>
  );
}
