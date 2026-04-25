import streamlit as st
import pandas as pd
import mysql.connector
import pydeck as pdk

# ==========================================
# PAGE CONFIG
# ==========================================
st.set_page_config(
    page_title="Cambodia Weather Intelligence",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================================
# PROFESSIONAL CSS DESIGN SYSTEM
# ==========================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,600;1,9..40,300&display=swap');

    :root {
        --bg-base:        #0a0c10;
        --bg-surface:     #111318;
        --bg-elevated:    #181c24;
        --border-subtle:  rgba(255,255,255,0.06);
        --border-default: rgba(255,255,255,0.10);
        --accent-primary: #f97316;
        --accent-secondary:#fbbf24;
        --accent-cool:    #38bdf8;
        --text-primary:   #f1f5f9;
        --text-secondary: #8b97a8;
        --text-muted:     #4a5568;
        --danger-bg:      rgba(239,68,68,0.08);
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
        background-color: var(--bg-base);
        color: var(--text-primary);
    }
    .stApp {
        background: var(--bg-base);
        background-image:
            radial-gradient(ellipse 80% 50% at 110% -10%, rgba(249,115,22,0.10) 0%, transparent 60%),
            radial-gradient(ellipse 60% 40% at -10% 80%, rgba(56,189,248,0.06) 0%, transparent 60%);
    }
    .main .block-container { padding: 2rem 2.5rem 4rem; max-width: 1400px; }

    /* SIDEBAR */
    section[data-testid="stSidebar"] {
        background-color: var(--bg-surface) !important;
        border-right: 1px solid var(--border-default);
    }
    section[data-testid="stSidebar"] .block-container { padding: 1.5rem 1.2rem; }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        font-family: 'Space Mono', monospace;
        font-size: 0.70rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--text-muted);
        margin-bottom: 0.8rem;
    }

    /* PAGE HEADER */
    .page-header {
        display: flex; flex-direction: column; gap: 4px;
        padding: 2rem 0 1.6rem;
        border-bottom: 1px solid var(--border-subtle);
        margin-bottom: 2rem;
    }
    .page-header .eyebrow {
        font-family: 'Space Mono', monospace;
        font-size: 0.68rem; letter-spacing: 0.18em;
        text-transform: uppercase; color: var(--accent-primary);
    }
    .page-header h1 {
        font-family: 'DM Sans', sans-serif;
        font-size: 2.2rem; font-weight: 600;
        color: var(--text-primary); margin: 0;
        line-height: 1.1; letter-spacing: -0.03em;
    }
    .page-header .subtitle {
        font-size: 0.90rem; color: var(--text-secondary);
        font-weight: 300; margin-top: 2px;
    }

    /* METRIC CARDS */
    div[data-testid="stMetric"] {
        background: var(--bg-surface);
        border: 1px solid var(--border-default);
        border-radius: 12px;
        padding: 1.2rem 1.4rem !important;
        position: relative; overflow: hidden;
        transition: border-color 0.2s ease;
    }
    div[data-testid="stMetric"]:hover { border-color: rgba(249,115,22,0.35); }
    div[data-testid="stMetric"]::before {
        content: ''; position: absolute;
        top: 0; left: 0; right: 0; height: 2px;
        background: linear-gradient(90deg, var(--accent-primary), var(--accent-secondary));
        opacity: 0.6;
    }
    div[data-testid="stMetric"] label {
        font-family: 'Space Mono', monospace !important;
        font-size: 0.65rem !important; letter-spacing: 0.12em !important;
        text-transform: uppercase !important; color: var(--text-muted) !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-size: 1.9rem !important; font-weight: 600 !important;
        color: var(--text-primary) !important; line-height: 1.2 !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-size: 0.80rem !important; color: var(--text-secondary) !important;
    }

    /* SECTION LABELS */
    h2, h3 {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important; color: var(--text-primary) !important;
        letter-spacing: -0.02em;
    }
    .section-label {
        font-family: 'Space Mono', monospace;
        font-size: 0.65rem; letter-spacing: 0.14em;
        text-transform: uppercase; color: var(--accent-primary);
        margin-bottom: 0.5rem;
    }

    /* ALERT */
    div[data-testid="stAlert"] {
        background: var(--danger-bg) !important;
        border: 1px solid rgba(239,68,68,0.25) !important;
        border-radius: 10px !important; color: #fca5a5 !important;
    }

    /* MAP & CHART CONTAINERS */
    div[data-testid="stArrowVegaLiteChart"],
    div[data-testid="stDeckGlJsonChart"] {
        background: var(--bg-surface);
        border: 1px solid var(--border-default);
        border-radius: 14px; overflow: hidden;
    }

    /* EXPANDER */
    details {
        background: var(--bg-surface) !important;
        border: 1px solid var(--border-default) !important;
        border-radius: 12px !important;
    }
    summary {
        font-family: 'Space Mono', monospace;
        font-size: 0.72rem; letter-spacing: 0.10em;
        text-transform: uppercase; color: var(--text-secondary) !important;
        padding: 0.9rem 1.2rem !important;
    }

    /* DATAFRAME */
    div[data-testid="stDataFrame"] {
        border-radius: 10px; overflow: hidden;
        border: 1px solid var(--border-subtle);
    }

    /* MULTISELECT */
    div[data-testid="stMultiSelect"] > div {
        background: var(--bg-elevated) !important;
        border: 1px solid var(--border-default) !important;
        border-radius: 8px !important;
    }

    /* DIVIDER */
    hr { border-color: var(--border-subtle) !important; margin: 2rem 0 !important; }

    /* LIVE STATUS PILL */
    .status-pill {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 3px 10px;
        background: rgba(34,197,94,0.08);
        border: 1px solid rgba(34,197,94,0.25);
        border-radius: 100px;
        font-family: 'Space Mono', monospace;
        font-size: 0.62rem; letter-spacing: 0.10em;
        text-transform: uppercase; color: #4ade80;
    }
    .status-dot {
        width: 6px; height: 6px; background: #22c55e;
        border-radius: 50%; animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50%       { opacity: 0.3; }
    }

    /* COLOR LEGEND */
    .legend-item {
        display: flex; align-items: center; gap: 10px;
        padding: 6px 0;
        font-family: 'Space Mono', monospace;
        font-size: 0.62rem; color: #8b97a8;
        border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .legend-swatch {
        width: 24px; height: 10px;
        border-radius: 3px; flex-shrink: 0;
    }

    /* SCROLLBAR */
    ::-webkit-scrollbar { width: 4px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--border-default); border-radius: 2px; }
    </style>
""", unsafe_allow_html=True)


# ==========================================
# COLOR GRADIENT: Blue (cold) → Cyan → Yellow → Orange → Red (hot)
# Range: 25°C (min) → 42°C (max)
# ==========================================
def temp_to_rgb(temp, t_min=25, t_max=42):
    ratio = max(0.0, min(1.0, (temp - t_min) / (t_max - t_min)))

    if ratio < 0.25:        # Blue → Cyan
        t = ratio / 0.25
        r, g, b = 0, int(t * 200), 255
    elif ratio < 0.5:       # Cyan → Yellow
        t = (ratio - 0.25) / 0.25
        r, g, b = int(t * 255), 200, int((1 - t) * 255)
    elif ratio < 0.75:      # Yellow → Orange
        t = (ratio - 0.5) / 0.25
        r, g, b = 255, int((1 - t) * 200), 0
    else:                   # Orange → Red
        t = (ratio - 0.75) / 0.25
        r, g, b = 255, int((1 - t) * 120), 0

    return [r, g, b, 220]


# ==========================================
# DATA ENGINE
# ==========================================
# @st.cache_data(ttl=60)
# def get_receipts():
#     conn = mysql.connector.connect(
#         host="localhost", port=3307, database="airflow",
#         user="airflow", password="airflow"
#     )
#     df = pd.read_sql("SELECT * FROM cambodia_weather ORDER BY timestamp DESC", conn)
#     conn.close()
#     df['timestamp'] = pd.to_datetime(df['timestamp'])
#     df = df.sort_values('timestamp', ascending=False).drop_duplicates('province')
#     return df
# ==========================================
# DATA ENGINE
# ==========================================
# ==========================================
# DATA ENGINE (កែឱ្យត្រូវស្តង់ដារ Security)
# ==========================================
@st.cache_data(ttl=60)
def get_receipts():
    db = st.secrets["mysql"]
    conn = mysql.connector.connect(
        host=db["host"],
        port=db["port"],
        database=db["database"],
        user=db["user"],
        password=db["password"],
        ssl_disabled=False
    )
    df = pd.read_sql("SELECT * FROM cambodia_weather ORDER BY timestamp DESC", conn)
    conn.close()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'] + pd.Timedelta(hours=7)
    df = df.sort_values('timestamp', ascending=False).drop_duplicates('province')
    return df
df_raw = get_receipts()


# ==========================================
# SIDEBAR
# ==========================================
with st.sidebar:
    st.markdown("### 🛰 Filters")
    st.markdown("---")

    all_provinces = sorted(df_raw['province'].unique())
    selected_provinces = st.multiselect(
        "Provinces",
        options=all_provinces,
        default=all_provinces,
        help="Filter the dashboard by province"
    )

    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.markdown(f"""
        <div style="font-family:'Space Mono',monospace; font-size:0.62rem;
                    color:#4a5568; line-height:2.2; margin-bottom:1.4rem;">
            TOTAL NODES &nbsp; <span style="color:#f1f5f9">{len(df_raw)}</span><br>
            SELECTED &nbsp;&nbsp;&nbsp;&nbsp; <span style="color:#f97316">{len(selected_provinces)}</span>
        </div>
    """, unsafe_allow_html=True)

    # Temperature color legend
    st.markdown("### 🌡 Temp Scale")
    st.markdown("""
        <div style="margin-top:8px">
            <div class="legend-item">
                <div class="legend-swatch" style="background:linear-gradient(90deg,#dc2626,#ef4444)"></div>
                <span>≥ 38°C &nbsp; Extreme</span>
            </div>
            <div class="legend-item">
                <div class="legend-swatch" style="background:linear-gradient(90deg,#f97316,#fb923c)"></div>
                <span>33–38°C &nbsp; Hot</span>
            </div>
            <div class="legend-item">
                <div class="legend-swatch" style="background:linear-gradient(90deg,#eab308,#fbbf24)"></div>
                <span>29–33°C &nbsp; Warm</span>
            </div>
            <div class="legend-item">
                <div class="legend-swatch" style="background:linear-gradient(90deg,#06b6d4,#22d3ee)"></div>
                <span>25–29°C &nbsp; Mild</span>
            </div>
            <div class="legend-item">
                <div class="legend-swatch" style="background:linear-gradient(90deg,#1d4ed8,#3b82f6)"></div>
                <span>≤ 25°C &nbsp;&nbsp; Cool</span>
            </div>
        </div>
    """, unsafe_allow_html=True)


# ==========================================
# APPLY FILTERS + COLOR
# ==========================================
df = df_raw[df_raw['province'].isin(selected_provinces)].copy()
df['color'] = df['temperature'].apply(temp_to_rgb)


# ==========================================
# ALERTS
# ==========================================
extreme_heat = df[df['temperature'] >= 38]
if not extreme_heat.empty:
    for _, row in extreme_heat.iterrows():
        st.toast(f"🥵 {row['province']} — {row['temperature']}°C", icon="⚠️")
    st.error(f"⚠️  Extreme heat detected in **{len(extreme_heat)} province(s)**. Temperature ≥ 38°C. Stay hydrated.")


# ==========================================
# PAGE HEADER
# ==========================================
# ✅ ថ្មី — handle NULL
ts = df_raw['timestamp'].max()
if pd.isnull(ts):
    latest_date = "No data yet"
else:
    latest_date = ts.strftime("%d %b %Y · %H:%M")

st.markdown(f"""
    <div class="page-header">
        <span class="eyebrow">Real-time · Cambodia</span>
        <h1>Weather Intelligence</h1>
        <span class="subtitle">
            Atmospheric monitoring across 25 provinces &nbsp;|&nbsp; Last sync: {latest_date}
            &nbsp;&nbsp;
            <span class="status-pill">
                <span class="status-dot"></span> Live
            </span>
        </span>
    </div>
""", unsafe_allow_html=True)


# ==========================================
# METRIC CARDS
# ==========================================

if df.empty:
    st.warning("⚠️ No data yet — Please trigger the DAG in Airflow first!")
    st.info("Go to: http://localhost:8081 → cambodia_weather_etl → ▶️ Trigger")
    st.stop()

hottest = df.loc[df['temperature'].idxmax()]
coldest = df.loc[df['temperature'].idxmin()]
avg_humidity = int(df['humidity'].mean())
active_nodes = len(df)

m1, m2, m3, m4 = st.columns(4)
with m1:
    st.metric("🥵 Hottest Province", f"{hottest['temperature']}°C", hottest['province'])
with m2:
    st.metric("🥶 Coolest Province", f"{coldest['temperature']}°C", coldest['province'])
with m3:
    st.metric("💧 Avg. Humidity", f"{avg_humidity}%")
with m4:
    st.metric("🛰 Active Nodes", f"{active_nodes} / 25")


# ==========================================
# 3D COLUMN MAP — temperature color gradient
# ==========================================
st.write("")
st.markdown('<p class="section-label">Spatial View</p>', unsafe_allow_html=True)
st.subheader("3D Temperature Landscape")

column_layer = pdk.Layer(
    "ColumnLayer",
    data=df,
    get_position="[longitude, latitude]",
    get_elevation="temperature",
    elevation_scale=1500,
    radius=10000,
    get_fill_color="color",      # ← precomputed RGB list from temp_to_rgb()
    pickable=True,
    auto_highlight=True,
)

st.pydeck_chart(pdk.Deck(
    map_style='https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    initial_view_state=pdk.ViewState(
        latitude=12.5657, longitude=104.9910,
        zoom=6.5, pitch=48, bearing=0
    ),
    layers=[column_layer],
    tooltip={
        "html": (
            "<b style='font-size:13px'>{province}</b><br/>"
            "<span style='font-size:20px; font-weight:700'>{temperature}°C</span>"
        ),
        "style": {
            "background": "#111318",
            "border": "1px solid rgba(249,115,22,0.4)",
            "border-radius": "8px",
            "color": "#f1f5f9",
            "padding": "10px 14px",
            "font-family": "DM Sans, sans-serif"
        }
    }
))


# ==========================================
# RANKING CHART
# ==========================================
st.write("")
st.markdown('<p class="section-label">Province Ranking</p>', unsafe_allow_html=True)
st.subheader("Temperature by Province")
st.bar_chart(
    df.set_index('province')['temperature'].sort_values(ascending=False),
    color="#f97316"
)

st.divider()

# ==========================================
# RAW DATA TABLE
# ==========================================
with st.expander("Raw Data — Full Table"):
    st.dataframe(
        df.drop(columns=['color']),
        use_container_width=True
    )