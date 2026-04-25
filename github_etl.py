import requests
import mysql.connector
import os
from datetime import datetime, timedelta

API_KEY = os.environ.get("OPENWEATHER_API_KEY")

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
    {"name": "Tboung Khmum", "lat": 11.8891, "lon": 105.8760},
]

def fetch_and_load():
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
    for p in CAMBODIA_PROVINCES:
        url = (
            f"https://api.openweathermap.org/data/2.5/weather"
            f"?lat={p['lat']}&lon={p['lon']}"
            f"&appid={API_KEY}&units=metric&lang=en"
        )
        try:
            r = requests.get(url, timeout=10)
            d = r.json()
            if r.status_code == 200:
                temp = d["main"]["temp"]
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

                timestamp = (datetime.utcnow() + timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')

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
                    p["name"], p["name"], "Cambodia",
                    d["coord"]["lat"], d["coord"]["lon"],
                    round(temp, 1), round(d["main"]["feels_like"], 1),
                    round(d["main"]["temp_min"], 1), round(d["main"]["temp_max"], 1),
                    d["main"]["humidity"], d["main"]["pressure"],
                    d["weather"][0]["description"], d["weather"][0]["main"],
                    round(d["wind"]["speed"], 1), d["wind"].get("deg", 0),
                    d["clouds"]["all"], d.get("visibility", 0),
                    heat_level, timestamp
                ))
                inserted += 1
                print(f"✅ {p['name']} — {temp}°C")
        except Exception as e:
            print(f"⚠️ {p['name']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n✅ Done! {inserted}/25 provinces inserted.")

if __name__ == "__main__":
    fetch_and_load()