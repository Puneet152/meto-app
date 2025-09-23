import io
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Tuple

import requests
import pandas as pd
import matplotlib.pyplot as plt

from fastapi import FastAPI, Query, HTTPException, Response
from fastapi.responses import StreamingResponse

# ReportLab
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet

# ---------------------------
# FastAPI app
# ---------------------------
app = FastAPI(title="Weather Service")

# ---------------------------
# Database setup
# ---------------------------
DB_PATH = "weather.db"


def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            temperature REAL,
            humidity REAL,
            latitude REAL,
            longitude REAL,
            UNIQUE(timestamp, latitude, longitude)
        )
        """
    )
    conn.commit()
    conn.close()


init_db()


def insert_weather_rows(rows: List[Tuple[str, float, float, float, float]]):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO weather (timestamp, temperature, humidity, latitude, longitude) VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    inserted = conn.total_changes
    conn.close()
    return inserted


def fetch_last_48h(lat: Optional[float] = None, lon: Optional[float] = None):
    now_utc = datetime.now(timezone.utc)
    since = now_utc - timedelta(hours=48)
    since_iso = since.replace(microsecond=0).isoformat()

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    if lat is None or lon is None:
        cur.execute(
            "SELECT timestamp, temperature, humidity, latitude, longitude FROM weather WHERE timestamp >= ? ORDER BY timestamp ASC",
            (since_iso,),
        )
    else:
        cur.execute(
            "SELECT timestamp, temperature, humidity, latitude, longitude FROM weather WHERE timestamp >= ? AND latitude = ? AND longitude = ? ORDER BY timestamp ASC",
            (since_iso, lat, lon),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------------------------
# Open-Meteo fetcher (forecast with past_days)
# ---------------------------
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"


def fetch_open_meteo(lat: float, lon: float, past_days: int = 2, timezone_str: str = "UTC"):
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relative_humidity_2m",
        "past_days": past_days,  # <-- include last N days
        "timezone": timezone_str,
    }
    resp = requests.get(OPEN_METEO_FORECAST, params=params, timeout=30)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Open-Meteo API error: {resp.status_code}")
    return resp.json()


# ---------------------------
# Endpoints
# ---------------------------

@app.get("/weather-report")
def weather_report(lat: float = Query(...), lon: float = Query(...)):
    """Fetch past 2 days from Open-Meteo forecast and store into SQLite."""
    data = fetch_open_meteo(lat, lon, past_days=2, timezone_str="UTC")

    hourly = data.get("hourly")
    if not hourly:
        raise HTTPException(status_code=502, detail="Open-Meteo response missing 'hourly' key.")

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    hums = hourly.get("relative_humidity_2m", [])

    length = min(len(times), len(temps), len(hums))
    rows = [
        (times[i], float(temps[i]), float(hums[i]), float(lat), float(lon))
        for i in range(length)
    ]

    inserted = insert_weather_rows(rows)

    return {
        "message": "Weather data fetched and stored.",
        "location": {"lat": lat, "lon": lon},
        "requested_records": len(rows),
        "db_inserted_rows": inserted,
    }


@app.get("/export/excel")
def export_excel(lat: Optional[float] = Query(None), lon: Optional[float] = Query(None)):
    """Return last 48 hours of data as Excel (.xlsx)."""
    rows = fetch_last_48h(lat, lon)

    df = pd.DataFrame(
        rows,
        columns=["timestamp", "temperature_2m", "relative_humidity_2m", "latitude", "longitude"],
    )

    buf = io.BytesIO()
    df.to_excel(buf, index=False, sheet_name="weather")
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=weather_last_48h.xlsx"},
    )


@app.get("/export/pdf")
def export_pdf(lat: Optional[float] = Query(None), lon: Optional[float] = Query(None)):
    """Generate a PDF with title, metadata, and a chart."""
    rows = fetch_last_48h(lat, lon)
    buf = io.BytesIO()

    if not rows:
        # PDF with "no data"
        doc = SimpleDocTemplate(buf)
        styles = getSampleStyleSheet()
        story = [
            Paragraph("Weather Report", styles["Title"]),
            Paragraph("No data available for last 48 hours.", styles["Normal"]),
        ]
        doc.build(story)
        buf.seek(0)
        return Response(
            content=buf.getvalue(),
            media_type="application/pdf",
            headers={"Content-Disposition": "attachment; filename=weather_report.pdf"},
        )

    # DataFrame
    df = pd.DataFrame(
        rows,
        columns=["timestamp", "temperature_2m", "relative_humidity_2m", "latitude", "longitude"],
    )
    df["timestamp_parsed"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # --- Chart ---
    plt.figure(figsize=(10, 4.5))
    ax1 = plt.gca()
    ax1.plot(df["timestamp_parsed"], df["temperature_2m"], label="Temperature (°C)", linewidth=1)
    ax1.set_ylabel("Temperature (°C)")
    ax1.set_xlabel("Time")

    ax2 = ax1.twinx()
    ax2.plot(df["timestamp_parsed"], df["relative_humidity_2m"], label="Humidity (%)", linestyle="--", linewidth=1)
    ax2.set_ylabel("Relative Humidity (%)")

    lines_1, labels_1 = ax1.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax1.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left", fontsize="small")

    plt.tight_layout()
    chart_buf = io.BytesIO()
    plt.savefig(chart_buf, format="png", dpi=150)
    plt.close()
    chart_buf.seek(0)

    # --- PDF ---
    doc = SimpleDocTemplate(buf)
    styles = getSampleStyleSheet()
    story = []

    now = datetime.utcnow()
    date_range = f"{(now - timedelta(hours=48)).strftime('%Y-%m-%d %H:%M UTC')} to {now.strftime('%Y-%m-%d %H:%M UTC')}"
    location = f"lat={lat}, lon={lon}" if lat and lon else "All locations"

    story.append(Paragraph("Weather Report", styles["Title"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f"<b>Location:</b> {location}", styles["Normal"]))
    story.append(Paragraph(f"<b>Date range:</b> {date_range}", styles["Normal"]))
    story.append(Spacer(1, 20))

    img = Image(chart_buf, width=500, height=250)
    story.append(img)

    doc.build(story)
    buf.seek(0)

    return Response(
        content=buf.getvalue(),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=weather_report.pdf"},
    )


# ---------------------------
# Root endpoint (optional)
# ---------------------------
@app.get("/")
def root():
    return {
        "message": "Weather API is running 🚀",
        "endpoints": {
            "/weather-report?lat=47.37&lon=8.55": "Fetch & store last 2 days of weather data",
            "/export/excel": "Download last 48h weather data as Excel",
            "/export/pdf": "Download last 48h weather data as PDF report",
        },
    }
