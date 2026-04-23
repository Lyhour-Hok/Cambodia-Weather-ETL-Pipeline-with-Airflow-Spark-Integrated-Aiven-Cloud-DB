from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import requests
import json
import os

# ==============================
# CONFIG
# ==============================
API_KEY = "beef9bf9339a69329d747d69842ba8de"

# គ្រប់ខេត្ត-ក្រុងទាំងអស់នៅកម្ពុជា (25 ខេត្ត-ក្រុង)
CAMBODIA_PROVINCES = [
    {"name": "Phnom Penh", "lat": 11.5564, "lon": 104.9282},
    {"name": "Banteay Meanchey", "lat": 13.6673, "lon": 102.8975},
    {"name": "Battambang", "lat": 13.0287, "lon": 103.2080},
    {"name": "Kampong Cham", "lat": 12.0000, "lon": 105.4500},
    {"name": "Kampong Chhnang", "lat": 12.2500, "lon": 104.6667},
    {"name": "Kampong Speu", "lat": 11.4533, "lon": 104.3206},
    {"name": "Kampong Thom", "lat": 12.7111, "lon": 104.8887},
    {"name": "Kampot", "lat": 10.6104, "lon": 104.1815},
    {"name": "Kandal", "lat": 11.4550, "lon": 104.9390},
    {"name": "Kep", "lat": 10.4828, "lon": 104.3167},
    {"name": "Koh Kong", "lat": 11.6153, "lon": 103.3587},
    {"name": "Kratié", "lat": 12.4881, "lon": 106.0188},
    {"name": "Mondulkiri", "lat": 12.4558, "lon": 107.1881},
    {"name": "Oddar Meanchey", "lat": 14.1818, "lon": 103.5000},
    {"name": "Pailin", "lat": 12.8489, "lon": 102.6093},
    {"name": "Preah Sihanouk", "lat": 10.6253, "lon": 103.5234},
    {"name": "Preah Vihear", "lat": 14.0000, "lon": 104.8333},
    {"name": "Prey Veng", "lat": 11.4868, "lon": 105.3253},
    {"name": "Pursat", "lat": 12.5388, "lon": 103.9192},
    {"name": "Ratanakiri", "lat": 13.7394, "lon": 107.0028},
    {"name": "Siem Reap", "lat": 13.3618, "lon": 103.8590},
    {"name": "Stung Treng", "lat": 13.5259, "lon": 105.9683},
    {"name": "Svay Rieng", "lat": 11.0878, "lon": 105.7993},
    {"name": "Takéo", "lat": 10.9908, "lon": 104.7849},
    {"name": "Tboung Khmum", "lat": 11.8891, "lon": 105.8760}
]

# ==============================
# STEP 1: EXTRACT
# ==============================
# ==============================
# STEP 1: EXTRACT (Pro Version)
# ==============================
def extract_weather():
    import requests
    import json
    from datetime import datetime

    all_data = []

    for province_data in CAMBODIA_PROVINCES:
        city_name = province_data["name"]
        lat = province_data["lat"]
        lon = province_data["lon"]

        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={lat}&lon={lon}"
            f"&appid={API_KEY}"
            f"&units=metric"
            f"&lang=en"
        )
        
        try:
            response = requests.get(url, timeout=10)
            data = response.json()

            if response.status_code == 200:
                all_data.append({
                    "city":        city_name,  # ប្រើឈ្មោះពី Dictionary របស់យើង
                    "province":    city_name,  # ប្រើឈ្មោះពី Dictionary របស់យើង
                    "country":     "Cambodia",
                    "latitude":    data["coord"]["lat"],
                    "longitude":   data["coord"]["lon"],
                    "temperature": data["main"]["temp"],
                    "feels_like":  data["main"]["feels_like"],
                    "temp_min":    data["main"]["temp_min"],
                    "temp_max":    data["main"]["temp_max"],
                    "humidity":    data["main"]["humidity"],
                    "pressure":    data["main"]["pressure"],
                    "weather":     data["weather"][0]["description"],
                    "weather_main":data["weather"][0]["main"],
                    "wind_speed":  data["wind"]["speed"],
                    "wind_deg":    data["wind"].get("deg", 0),
                    "cloudiness":  data["clouds"]["all"],
                    "visibility":  data.get("visibility", 0),
                    "timestamp": (datetime.utcnow() + timedelta(hours=7)).isoformat()
                })
                print(f"✅ {city_name} — {data['main']['temp']}°C, {data['weather'][0]['description']}")
            else:
                print(f"❌ Failed: {city_name} → {data.get('message')}")

        except Exception as e:
            print(f"⚠️ Error for {city_name}: {str(e)}")

    with open("/tmp/weather_raw.json", "w") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Extract done! {len(all_data)}/{len(CAMBODIA_PROVINCES)} cities collected.")

# ==============================
# STEP 2: TRANSFORM
# ==============================
def transform_weather():
    with open("/tmp/weather_raw.json", "r") as f:
        data = json.load(f)

    cleaned = []
    for row in data:
        # កំណត់ heat level
        temp = row["temperature"]
        if temp >= 38:
            heat_level = "Extreme Heat 🔴"
        elif temp >= 35:
            heat_level = "Very Hot 🟠"
        elif temp >= 30:
            heat_level = "Hot 🟡"
        elif temp >= 25:
            heat_level = "Warm 🟢"
        else:
            heat_level = "Cool 🔵"

        cleaned.append({
            "city":         row["city"].strip(),
            "province":     row["province"].strip(),
            "country":      row["country"],
            "latitude":     row["latitude"],
            "longitude":    row["longitude"],
            "temperature":  round(row["temperature"], 1),
            "feels_like":   round(row["feels_like"], 1),
            "temp_min":     round(row["temp_min"], 1),
            "temp_max":     round(row["temp_max"], 1),
            "humidity":     int(row["humidity"]),
            "pressure":     int(row["pressure"]),
            "weather":      row["weather"].lower().strip(),
            "weather_main": row["weather_main"],
            "wind_speed":   round(row["wind_speed"], 1),
            "wind_deg":     int(row["wind_deg"]),
            "cloudiness":   int(row["cloudiness"]),
            "visibility":   int(row["visibility"]),
            "heat_level":   heat_level,
            "timestamp":    row["timestamp"]
        })

    # Sort by temperature (ក្តៅបំផុតដំបូង)
    cleaned.sort(key=lambda x: x["temperature"], reverse=True)

    with open("/tmp/weather_clean.json", "w") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)

    print(f"✅ Transform done! {len(cleaned)} provinces cleaned.")
    print(f"🌡️ Hottest: {cleaned[0]['province']} — {cleaned[0]['temperature']}°C")
    print(f"❄️ Coolest: {cleaned[-1]['province']} — {cleaned[-1]['temperature']}°C")


# ==============================
# STEP 3: LOAD
# ==============================
# ==============================
# STEP 3: LOAD (MySQL Version)
# ==============================
def load_weather():
    import mysql.connector
    import json

    with open("/tmp/weather_clean.json", "r") as f:
        data = json.load(f)

    # Note: Make sure these match your MySQL container setup!
    conn = mysql.connector.connect(
    host=os.environ.get("AIVEN_HOST"),
    port=int(os.environ.get("AIVEN_PORT", 12992)),
    database=os.environ.get("AIVEN_DB"),
    user=os.environ.get("AIVEN_USER"),
    password=os.environ.get("AIVEN_PASSWORD"),
    ssl_disabled=False,
    ssl_verify_cert=False,
    ssl_verify_identity=False
)
    cur = conn.cursor()

    # Changed SERIAL to INT AUTO_INCREMENT and NOW() to CURRENT_TIMESTAMP
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cambodia_weather (
            id           INT AUTO_INCREMENT PRIMARY KEY,
            city         VARCHAR(100),
            province     VARCHAR(100),
            country      VARCHAR(50),
            latitude     FLOAT,
            longitude    FLOAT,
            temperature  FLOAT,
            feels_like   FLOAT,
            temp_min     FLOAT,
            temp_max     FLOAT,
            humidity     INT,
            pressure     INT,
            weather      VARCHAR(200),
            weather_main VARCHAR(100),
            wind_speed   FLOAT,
            wind_deg     INT,
            cloudiness   INT,
            visibility   INT,
            heat_level   VARCHAR(50),
            timestamp    TIMESTAMP,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    inserted = 0
    for row in data:
        # Syntax %s works for mysql-connector-python as well
        cur.execute("""
            INSERT INTO cambodia_weather (
                city, province, country, latitude, longitude,
                temperature, feels_like, temp_min, temp_max,
                humidity, pressure, weather, weather_main,
                wind_speed, wind_deg, cloudiness, visibility,
                heat_level, timestamp
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s
            )
        """, (
            row["city"], row["province"], row["country"],
            row["latitude"], row["longitude"],
            row["temperature"], row["feels_like"],
            row["temp_min"], row["temp_max"],
            row["humidity"], row["pressure"],
            row["weather"], row["weather_main"],
            row["wind_speed"], row["wind_deg"],
            row["cloudiness"], row["visibility"],
            row["heat_level"], row["timestamp"]
        ))
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Load done! {inserted} provinces inserted into MySQL database.")

# ==============================
# DAG DEFINITION
# ==============================
with DAG(
    dag_id="cambodia_weather_etl",
    description="ETL pipeline — Weather for all Cambodia provinces",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather", "cambodia", "etl"]
) as dag:

    t1 = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather
    )

    t2 = SparkSubmitOperator(
    task_id="transform_weather",
    application="/opt/airflow/spark_jobs/transform.py",
    conn_id="spark_default",
    verbose=True
)

    t3 = PythonOperator(
        task_id="load_to_database",
        python_callable=load_weather
    )

    t1 >> t2 >> t3