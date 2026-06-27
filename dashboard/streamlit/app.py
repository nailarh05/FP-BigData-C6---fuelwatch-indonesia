import os, json, redis, psycopg2, requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import folium
import streamlit as st
from streamlit_folium import st_folium
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="FuelWatch Indonesia",
    page_icon="⛽", layout="wide",
    initial_sidebar_state="expanded"
)

BBM_DATE   = os.getenv('BBM_EVENT_DATE', '2026-06-10')
BBM_DT     = datetime.strptime(BBM_DATE, '%Y-%m-%d')
BBM_UNIX   = BBM_DT.timestamp() * 1000  # untuk plotly vline

CITY_COORDS = {
    'Jakarta':    {'lat': -6.2088,  'lon': 106.8456},
    'Surabaya':   {'lat': -7.2575,  'lon': 112.7521},
    'Yogyakarta': {'lat': -7.7956,  'lon': 110.3695},
}
CITIES_LIST = ['Jakarta', 'Surabaya', 'Yogyakarta']

LEVEL_COLORS = {0:'#2ECC71', 1:'#F1C40F', 2:'#E67E22', 3:'#E74C3C'}
LEVEL_LABELS = {0:'Lancar', 1:'Ramai Lancar', 2:'Padat', 3:'Macet'}
ZONE_COLORS  = {'Dampak Tinggi':'#E74C3C','Dampak Sedang':'#F1C40F','Dampak Rendah':'#2ECC71'}

PLOTLY_BEFORE       = '#5C8BC6'
PLOTLY_AFTER        = '#E0623D'
ACCENT              = '#F2A93B'
PLOTLY_LAYOUT_EXTRA = dict(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Inter, sans-serif', color='#C8CEDD'),
    margin=dict(t=50, l=10, r=10, b=10),
)

def get_congestion_level(index):
    if index < 20: return 0
    if index < 40: return 1
    if index < 60: return 2
    return 3

# ── Connections ────────────────────────────────────────────
@st.cache_resource
def get_redis():
    return redis.Redis(
        host=os.getenv('REDIS_HOST','redis'),
        port=int(os.getenv('REDIS_PORT',6379)),
        decode_responses=True
    )

@st.cache_resource
def get_db():
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB','fuelwatch'),
        user=os.getenv('POSTGRES_USER','fuelwatch'),
        password=os.getenv('POSTGRES_PASSWORD','fuelwatch123'),
        host=os.getenv('POSTGRES_HOST','postgres'),
        port=os.getenv('POSTGRES_PORT','5432'),
    )

# ── Data Loaders ───────────────────────────────────────────
def load_comparison():
    try:
        data = get_redis().get('comparison:all_cities')
        return json.loads(data) if data else {}
    except: return {}

def load_predictions(city=None):
    try:
        data = get_redis().get('ml:predictions')
        preds = json.loads(data) if data else {}
        return preds.get(city, {}) if city else preds
    except: return {}

def load_model_metrics():
    try:
        data = get_redis().get('ml:model_metrics')
        return json.loads(data) if data else {}
    except: return {}

def load_clusters(city=None):
    try:
        data = get_redis().get('ml:clusters')
        df = pd.read_json(data) if data else pd.DataFrame()
        if not df.empty and city and city != 'Semua':
            df = df[df['city'] == city]
        return df
    except: return pd.DataFrame()

def load_anomaly_summary():
    try:
        data = get_redis().get('ml:anomaly_summary')
        return json.loads(data) if data else {}
    except: return {}

def load_anomaly_events(city=None, limit=50):
    try:
        conn = get_db()
        where = f"AND city = '{city}'" if city and city != 'Semua' else ""
        df = pd.read_sql(
            f"SELECT city, road_name, recorded_at, congestion_index, zscore "
            f"FROM gold_anomalies WHERE 1=1 {where} ORDER BY ABS(zscore) DESC LIMIT %s",
            conn, params=(limit,)
        )
        return df
    except: return pd.DataFrame()

def load_latest_traffic():
    try:
        result = []
        for city in CITY_COORDS:
            data = get_redis().get(f'latest:{city}')
            if data: result.append(json.loads(data))
        return result
    except: return []

def load_traffic_history(city=None, limit=2000):
    try:
        conn = get_db()
        where = f"WHERE city = '{city}'" if city else ""
        # Coba silver dulu, fallback ke bronze
        try:
            df = pd.read_sql(
                f"SELECT city, road_name, current_speed, congestion_index, "
                f"congestion_level, period, recorded_at FROM silver_traffic {where} "
                f"ORDER BY recorded_at DESC LIMIT %s", conn, params=(limit,)
            )
        except:
            df = pd.DataFrame()
        if df.empty:
            df = pd.read_sql(
                f"SELECT city, road_name, current_speed, congestion_index, period, recorded_at, "
                f"CASE WHEN congestion_index<20 THEN 0 WHEN congestion_index<40 THEN 1 "
                f"WHEN congestion_index<60 THEN 2 ELSE 3 END AS congestion_level "
                f"FROM bronze_traffic {where} ORDER BY recorded_at DESC LIMIT %s",
                conn, params=(limit,)
            )
        df['recorded_at'] = pd.to_datetime(df['recorded_at'])
        return df
    except: return pd.DataFrame()

def load_hourly_pattern(city):
    try:
        data = get_redis().get(f'hourly:{city}')
        return json.loads(data) if data else {}
    except: return {}

def load_daily_summary(city=None):
    try:
        conn = get_db()
        where = f"WHERE city = '{city}'" if city and city != 'Semua' else ""
        df = pd.read_sql(
            f"SELECT * FROM gold_daily_summary {where} ORDER BY summary_date DESC, city LIMIT 15",
            conn
        )
        return df
    except: return pd.DataFrame()

def load_layer_counts():
    counts = {}
    try:
        conn = get_db()
        tables = ['bronze_traffic', 'silver_traffic', 'gold_city_comparison',
                  'gold_hourly_pattern', 'gold_daily_summary', 'gold_predictions',
                  'gold_model_metrics', 'gold_road_clusters', 'gold_anomalies']
        with conn.cursor() as cur:
            for t in tables:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {t}")
                    counts[t] = cur.fetchone()[0]
                except: counts[t] = None
    except: pass
    return counts

def get_osrm_route(lat1, lon1, lat2, lon2):
    """Ambil rute dari OSRM public API (gratis, tanpa API key)."""
    try:
        url = f"http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=full&geometries=geojson"
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
        route = data['routes'][0]
        coords = route['geometry']['coordinates']
        # OSRM returns [lon,lat], folium needs [lat,lon]
        latlons = [[c[1], c[0]] for c in coords]
        duration_min = round(route['duration'] / 60, 1)
        distance_km  = round(route['distance'] / 1000, 1)
        return latlons, duration_min, distance_km
    except:
        return None, None, None

# ── CSS ────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@500;700&display=swap');
:root {
    --bg:#0a0e17; --surface:#121927; --surface-2:#182236;
    --border:#232c42; --text:#e7ebf5; --text-dim:#8993ad;
    --accent:#f2a93b; --accent-dim:rgba(242,169,59,0.12);
    --before:#5c8bc6; --after:#e0623d;
    --lancar:#2ecc71; --ramai:#f1c40f; --padat:#e67e22; --macet:#e74c3c;
}
html, body, [class*="css"] { font-family:'Inter',sans-serif; }
.main { background-color:var(--bg); }
[data-testid="stMetricValue"] { font-family:'JetBrains Mono',monospace; }
.section-title {
    color:var(--accent); font-family:'JetBrains Mono',monospace;
    font-size:13px; font-weight:700; text-transform:uppercase;
    letter-spacing:1.5px; margin:0 0 2px 0; padding-bottom:10px;
    border-bottom:1px solid var(--border);
}
.section-sub { color:var(--text-dim); font-size:13px; margin:8px 0 18px 0; }
.metric-card {
    background:var(--surface); border:1px solid var(--border);
    border-radius:10px; padding:18px 20px; margin-bottom:12px; height:100%;
}
.metric-label { color:var(--text-dim); font-size:11px; font-weight:600;
    text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }
.metric-value { font-family:'JetBrains Mono',monospace;
    color:var(--text); font-size:25px; font-weight:700; line-height:1.25; }
.metric-sub { color:var(--text-dim); font-size:12px; margin-top:6px; line-height:1.4; }
.kpi-box {
    background:var(--surface); border:1px solid var(--border);
    border-left:3px solid var(--accent); border-radius:8px;
    padding:14px 18px; height:100%;
}
.kpi-label { color:var(--text-dim); font-size:10.5px; text-transform:uppercase;
    letter-spacing:1px; font-weight:600; }
.kpi-value { font-family:'JetBrains Mono',monospace; font-size:22px;
    font-weight:700; color:var(--text); margin-top:5px; }
.kpi-sub { color:var(--text-dim); font-size:11px; margin-top:4px; }
.info-banner {
    background:var(--accent-dim); border:1px solid var(--accent);
    border-radius:8px; padding:12px 14px; font-size:13px;
    color:var(--text); line-height:1.6;
}
.info-banner b { color:var(--accent); }
.bbm-card {
    background:var(--surface); border:1px solid var(--border);
    border-left:3px solid var(--after); border-radius:10px;
    padding:16px 18px; margin-bottom:10px; font-size:13px;
    line-height:1.7; color:var(--text);
}
.zone-card {
    border-radius:10px; padding:14px 16px; margin-bottom:8px;
    border:1px solid rgba(255,255,255,0.1);
}
.zone-title { font-weight:700; font-size:14px; margin-bottom:4px; }
.zone-desc  { font-size:12px; color:#ccc; line-height:1.5; }
.layer-badge {
    display:inline-block; padding:3px 10px; border-radius:20px;
    font-size:10px; font-weight:700; letter-spacing:0.5px; margin-right:6px;
    font-family:'JetBrains Mono',monospace;
}
.badge-bronze{background:#8a5a3a;color:#fff;}
.badge-silver{background:#9aa0a6;color:#111;}
.badge-gold{background:#d4af37;color:#111;}
</style>
""", unsafe_allow_html=True)

# ── UI Helpers ─────────────────────────────────────────────
def metric_card(label, value, sub=None, value_color=None):
    color_attr = f'style="color:{value_color}"' if value_color else ''
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ''
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" {color_attr}>{value}</div>
        {sub_html}
    </div>""", unsafe_allow_html=True)

def kpi_box(label, value, sub=""):
    return f"""<div class="kpi-box">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div>
    </div>"""

def section_header(title, subtitle=None):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="section-sub">{subtitle}</div>', unsafe_allow_html=True)

# ── SIDEBAR ────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/9/9f/Flag_of_Indonesia.svg", width=60)
    st.title("⛽ FuelWatch")
    st.caption("Monitoring Dampak Kenaikan BBM terhadap Mobilitas")
    st.divider()

    st.markdown("##### 📅 Kebijakan yang Dipantau")
    st.markdown("""
    <div class="info-banner">
        <b>Berlaku 10 Juni 2026</b><br>
        Pertamax: Rp12.300 → <b>Rp16.250</b>/L<br>
        Pertamax Green: Rp12.900 → <b>Rp17.000</b>/L
    </div>""", unsafe_allow_html=True)
    st.divider()

    selected_city = st.selectbox("🏙️ Filter Kota (Global)", ["Semua"] + CITIES_LIST)
    st.divider()

    st.markdown("##### 🧱 Tech Stack")
    st.caption("Kafka → Spark MLlib → PostgreSQL/Parquet → Redis → Streamlit")
    st.caption("🗺️ Peta: OpenStreetMap + Folium · Routing: OSRM")

    auto_refresh = st.checkbox("🔄 Auto Refresh (60s)", value=True)
    if st.button("🔄 Refresh Sekarang", use_container_width=True):
        st.rerun()
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ── HEADER ─────────────────────────────────────────────────
st.title("⛽ FuelWatch Indonesia")
st.caption("Sistem Monitoring & Analitik Dampak Kenaikan Harga BBM terhadap Pola Mobilitas Kota Besar")

# ── KPI STRIP ──────────────────────────────────────────────
_comparison = load_comparison()
_anomaly    = load_anomaly_summary()
_daily      = load_daily_summary()

if _comparison:
    _changes    = {c: d.get('change_pct', 0) for c, d in _comparison.items()}
    _worst_city = max(_changes, key=_changes.get)
    _worst_chg  = _changes[_worst_city]
    _avg_change = sum(_changes.values()) / len(_changes)
else:
    _worst_city, _worst_chg, _avg_change = "-", 0, 0

_anom_after  = _anomaly.get('rate_after_pct', 0)
_anom_delta  = (_anomaly.get('rate_after_pct',0) - _anomaly.get('rate_before_pct',0)) if _anomaly else 0
_extra_cost  = _daily['est_extra_fuel_cost_idr'].sum() if not _daily.empty else 0

kc1, kc2, kc3, kc4 = st.columns(4)
with kc1: st.markdown(kpi_box("Kota Paling Terdampak", _worst_city,
    f"Kemacetan +{_worst_chg:.1f} pp pasca BBM naik" if _comparison else "Menunggu data..."),
    unsafe_allow_html=True)
with kc2: st.markdown(kpi_box("Rata-rata Kenaikan Kemacetan", f"{_avg_change:+.1f} pp",
    "Rata-rata 3 kota, before → after"), unsafe_allow_html=True)
with kc3: st.markdown(kpi_box("Tingkat Anomali (After)", f"{_anom_after:.1f}%",
    f"{_anom_delta:+.1f} pp vs sebelum BBM" if _anomaly else "Menunggu..."), unsafe_allow_html=True)
with kc4: st.markdown(kpi_box("Est. Extra Biaya BBM", f"Rp {_extra_cost:,.0f}",
    "Total 3 kota, hari terakhir" if not _daily.empty else "Menunggu..."), unsafe_allow_html=True)

st.divider()

# ── TABS ───────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🗺️ Peta Real-Time",
    "📊 Before vs After BBM",
    "🤖 ML & Analitik Lanjutan",
    "⛽ Harga BBM",
    "🏗️ Arsitektur & Lakehouse",
])

# ══════════════════════════════════════════════════════
# TAB 1 — PETA REAL-TIME (OpenStreetMap + Routing)
# ══════════════════════════════════════════════════════
with tab1:
    section_header("🗺️ Kondisi Lalu Lintas Real-Time")

    # Filter kota di tab ini
    city_filter_map = st.selectbox(
        "🏙️ Tampilkan Kota", ["Semua"] + CITIES_LIST, key="city_map",
        index=(["Semua"] + CITIES_LIST).index(selected_city) if selected_city in ["Semua"] + CITIES_LIST else 0
    )

    latest = load_latest_traffic()
    cities_show = CITIES_LIST if city_filter_map == "Semua" else [city_filter_map]

    # Metric cards per kota
    cols = st.columns(len(cities_show))
    for i, city in enumerate(cities_show):
        city_data = next((d for d in latest if d['city'] == city), None)
        with cols[i]:
            if city_data:
                ci    = city_data['avg_congestion_index']
                spd   = city_data['avg_speed']
                level = get_congestion_level(ci)
                metric_card(city, LEVEL_LABELS[level],
                            f"Kemacetan {ci:.1f}% &nbsp;·&nbsp; Kecepatan {spd:.1f} km/h",
                            value_color=LEVEL_COLORS[level])
            else:
                metric_card(city, "Menunggu data...",
                            "Collector sedang mengambil data", value_color="#666")

    # Peta OpenStreetMap
    col_map, col_routing = st.columns([3, 2])
    with col_map:
        st.markdown("**🗺️ Peta Kemacetan — OpenStreetMap**")
        center_lat = CITY_COORDS[city_filter_map]['lat'] if city_filter_map != "Semua" else -7.0
        center_lon = CITY_COORDS[city_filter_map]['lon'] if city_filter_map != "Semua" else 109.5
        zoom       = 12 if city_filter_map != "Semua" else 6

        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=zoom,
            tiles='OpenStreetMap',
            min_zoom=5, max_zoom=17,
            max_bounds=True,
            max_lat=10, min_lat=-15,
            max_lon=125, min_lon=94,
        )

        for city in (CITIES_LIST if city_filter_map == "Semua" else [city_filter_map]):
            coords    = CITY_COORDS[city]
            city_data = next((d for d in latest if d['city'] == city), None)
            if city_data:
                for road in city_data.get('roads', []):
                    rl    = get_congestion_level(road['congestion_index'])
                    color = LEVEL_COLORS[rl]
                    folium.CircleMarker(
                        location=[road['lat'], road['lon']],
                        radius=10, color=color, fill=True, fill_opacity=0.85,
                        popup=folium.Popup(
                            f"<b style='color:#111'>{road['road_name']}</b><br>"
                            f"Kota: {city}<br>"
                            f"Status: <b>{LEVEL_LABELS[rl]}</b><br>"
                            f"Kecepatan: {road['current_speed']} km/h<br>"
                            f"Kemacetan: {road['congestion_index']:.1f}%",
                            max_width=220
                        )
                    ).add_to(m)
                ci    = city_data['avg_congestion_index']
                level = get_congestion_level(ci)
                color = LEVEL_COLORS[level]
                folium.Marker(
                    location=[coords['lat'] + 0.04, coords['lon']],
                    icon=folium.DivIcon(html=f"""
                        <div style="background:{color};color:#000;padding:3px 8px;
                        border-radius:4px;font-weight:bold;font-size:10px;white-space:nowrap;
                        box-shadow:0 2px 6px rgba(0,0,0,0.3)">{city}: {LEVEL_LABELS[level]}</div>
                    """)
                ).add_to(m)
            else:
                folium.Marker(
                    location=[coords['lat'], coords['lon']],
                    popup=f"{city} — Menunggu data",
                    icon=folium.Icon(color='gray')
                ).add_to(m)

        # Tambahkan legenda
        legend_html = """
        <div style="position:fixed;bottom:30px;left:30px;z-index:9999;
             background:white;padding:10px 14px;border-radius:8px;
             border:2px solid #ccc;font-size:12px;font-family:Arial">
            <b>Status Kemacetan</b><br>
            <span style="color:#2ECC71">⬤</span> Lancar (&lt;20%)<br>
            <span style="color:#F1C40F">⬤</span> Ramai Lancar (20-40%)<br>
            <span style="color:#E67E22">⬤</span> Padat (40-60%)<br>
            <span style="color:#E74C3C">⬤</span> Macet (&gt;60%)
        </div>"""
        m.get_root().html.add_child(folium.Element(legend_html))
        st_folium(m, width=None, height=430, returned_objects=[])

    with col_routing:
        st.markdown("**🧭 Routing Antar Titik (OSRM)**")
        st.caption("Pilih kota & dua titik jalan untuk melihat rute dan estimasi waktu tempuh")

        route_city = st.selectbox("Kota", CITIES_LIST, key="route_city")
        roads_in_city = [r['name'] for r in {
            'Jakarta': [
                {'name':'Jl. Sudirman','lat':-6.2088,'lon':106.8175},
                {'name':'Jl. Thamrin','lat':-6.1944,'lon':106.8229},
                {'name':'Jl. HR Rasuna Said','lat':-6.2258,'lon':106.8317},
                {'name':'Jl. Gatot Subroto','lat':-6.2335,'lon':106.8007},
                {'name':'Jl. TB Simatupang','lat':-6.2897,'lon':106.7753},
            ],
            'Surabaya': [
                {'name':'Jl. Ahmad Yani','lat':-7.3048,'lon':112.7373},
                {'name':'Jl. Basuki Rahmat','lat':-7.2659,'lon':112.7469},
                {'name':'Jl. Raya Darmo','lat':-7.2820,'lon':112.7313},
                {'name':'Jl. Pemuda','lat':-7.2575,'lon':112.7521},
                {'name':'Jl. MERR','lat':-7.2897,'lon':112.7897},
            ],
            'Yogyakarta': [
                {'name':'Jl. Malioboro','lat':-7.7925,'lon':110.3663},
                {'name':'Jl. Solo','lat':-7.7833,'lon':110.4166},
                {'name':'Jl. Magelang','lat':-7.7614,'lon':110.3631},
                {'name':'Ring Road Utara','lat':-7.7614,'lon':110.3897},
                {'name':'Jl. Parangtritis','lat':-7.8319,'lon':110.3631},
            ],
        }[route_city]]

        ROAD_COORDS = {
            'Jl. Sudirman':       (-6.2088, 106.8175), 'Jl. Thamrin':       (-6.1944, 106.8229),
            'Jl. HR Rasuna Said': (-6.2258, 106.8317), 'Jl. Gatot Subroto': (-6.2335, 106.8007),
            'Jl. TB Simatupang':  (-6.2897, 106.7753), 'Jl. Ahmad Yani':    (-7.3048, 112.7373),
            'Jl. Basuki Rahmat':  (-7.2659, 112.7469), 'Jl. Raya Darmo':    (-7.2820, 112.7313),
            'Jl. Pemuda':         (-7.2575, 112.7521), 'Jl. MERR':          (-7.2897, 112.7897),
            'Jl. Malioboro':      (-7.7925, 110.3663), 'Jl. Solo':          (-7.7833, 110.4166),
            'Jl. Magelang':       (-7.7614, 110.3631), 'Ring Road Utara':   (-7.7614, 110.3897),
            'Jl. Parangtritis':   (-7.8319, 110.3631),
        }

        origin_name = st.selectbox("📍 Titik Asal", roads_in_city, key="origin")
        dest_name   = st.selectbox("🏁 Titik Tujuan", [r for r in roads_in_city if r != origin_name], key="dest")

        if st.button("🗺️ Tampilkan Rute", use_container_width=True, type="primary"):
            olat, olon = ROAD_COORDS[origin_name]
            dlat, dlon = ROAD_COORDS[dest_name]
            with st.spinner("Mengambil rute dari OSRM..."):
                route_coords, dur, dist = get_osrm_route(olat, olon, dlat, dlon)

            if route_coords:
                st.success(f"✅ Rute ditemukan: {dist} km · ±{dur} menit")

                # Tampilkan rute di peta kecil
                rm = folium.Map(location=[(olat+dlat)/2, (olon+dlon)/2],
                                zoom_start=13, tiles='OpenStreetMap')
                folium.PolyLine(route_coords, color='#F2A93B', weight=5,
                                opacity=0.9, tooltip=f"{dist} km · {dur} menit").add_to(rm)
                folium.Marker([olat, olon], popup=f"Asal: {origin_name}",
                              icon=folium.Icon(color='green', icon='play')).add_to(rm)
                folium.Marker([dlat, dlon], popup=f"Tujuan: {dest_name}",
                              icon=folium.Icon(color='red', icon='stop')).add_to(rm)
                st_folium(rm, width=None, height=260, returned_objects=[])

                # Cek kemacetan di ruas tujuan
                city_data = next((d for d in latest if d['city'] == route_city), None)
                if city_data:
                    dest_road = next((r for r in city_data.get('roads',[])
                                      if r['road_name'] == dest_name), None)
                    if dest_road:
                        lvl = get_congestion_level(dest_road['congestion_index'])
                        st.info(f"📊 Kondisi tujuan saat ini: **{LEVEL_LABELS[lvl]}** "
                                f"({dest_road['congestion_index']:.1f}%) · {dest_road['current_speed']} km/h")
            else:
                st.warning("⚠️ Rute tidak bisa diambil (cek koneksi internet untuk akses OSRM)")

    # Tabel detail ruas jalan
    if latest:
        st.divider()
        st.markdown("#### 📋 Detail Per Ruas Jalan")
        rows = []
        for d in latest:
            if city_filter_map != "Semua" and d['city'] != city_filter_map:
                continue
            for road in d.get('roads', []):
                lvl = get_congestion_level(road['congestion_index'])
                rows.append({
                    'Kota': d['city'], 'Ruas Jalan': road['road_name'],
                    'Kecepatan (km/h)': road['current_speed'],
                    'Kemacetan (%)': road['congestion_index'],
                    'Status': LEVEL_LABELS[lvl],
                })
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════
# TAB 2 — BEFORE vs AFTER BBM
# ══════════════════════════════════════════════════════
with tab2:
    section_header("📊 Analisis Before vs After BBM Naik", "Kebijakan berlaku 10 Juni 2026")

    city_filter_t2 = st.selectbox(
        "🏙️ Filter Kota", ["Semua"] + CITIES_LIST, key="city_t2",
        index=(["Semua"] + CITIES_LIST).index(selected_city) if selected_city in ["Semua"] + CITIES_LIST else 0
    )

    comparison = load_comparison()
    cities_cmp = CITIES_LIST if city_filter_t2 == "Semua" else [city_filter_t2]

    if comparison:
        cols = st.columns(len(cities_cmp))
        for i, city in enumerate(cities_cmp):
            data = comparison.get(city, {})
            with cols[i]:
                before_ci = data.get('before', {}).get('avg_congestion', 0)
                after_ci  = data.get('after', {}).get('avg_congestion', 0)
                change    = data.get('change_pct', 0)
                arrow = "🔺" if change > 0 else "🔻"
                color = "#E74C3C" if change > 0 else "#2ECC71"
                metric_card(city, f"{arrow} {abs(change):.1f} pp",
                            f"Before {before_ci:.1f}% &nbsp;→&nbsp; After {after_ci:.1f}%",
                            value_color=color)

        # Bar chart
        fig = go.Figure()
        cities_data = [(c, comparison[c]) for c in cities_cmp if c in comparison]
        fig.add_trace(go.Bar(
            name='Sebelum BBM Naik', x=[c for c,_ in cities_data],
            y=[d['before']['avg_congestion'] for _,d in cities_data],
            marker_color=PLOTLY_BEFORE,
            text=[f"{d['before']['avg_congestion']:.1f}%" for _,d in cities_data],
            textposition='outside'
        ))
        fig.add_trace(go.Bar(
            name='Sesudah BBM Naik', x=[c for c,_ in cities_data],
            y=[d['after']['avg_congestion'] for _,d in cities_data],
            marker_color=PLOTLY_AFTER,
            text=[f"{d['after']['avg_congestion']:.1f}%" for _,d in cities_data],
            textposition='outside'
        ))
        fig.update_layout(title='Indeks Kemacetan Before vs After BBM Naik',
                          barmode='group', yaxis_title='Indeks Kemacetan (%)',
                          legend=dict(orientation='h', yanchor='bottom', y=1.02),
                          height=380, **PLOTLY_LAYOUT_EXTRA)
        st.plotly_chart(fig, use_container_width=True)

        # Pola per jam
        st.markdown("#### 🕐 Pola Kemacetan Per Jam")
        city_hourly = st.selectbox("Pilih Kota", CITIES_LIST, key='hourly_city')
        hourly = load_hourly_pattern(city_hourly)
        if hourly:
            hours    = list(range(24))
            before_h = [hourly.get('before', {}).get(str(h), {}).get('avg_congestion', 0) for h in hours]
            after_h  = [hourly.get('after',  {}).get(str(h), {}).get('avg_congestion', 0) for h in hours]
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=hours, y=before_h, name='Sebelum BBM Naik',
                                      line=dict(color=PLOTLY_BEFORE, width=2), fill='tozeroy',
                                      fillcolor='rgba(92,139,198,0.12)'))
            fig2.add_trace(go.Scatter(x=hours, y=after_h, name='Sesudah BBM Naik',
                                      line=dict(color=PLOTLY_AFTER, width=2), fill='tozeroy',
                                      fillcolor='rgba(224,98,61,0.12)'))
            fig2.add_vrect(x0=7, x1=9, fillcolor='rgba(242,169,59,0.08)', line_width=0,
                           annotation_text="Rush Pagi", annotation_font=dict(size=10, color=ACCENT))
            fig2.add_vrect(x0=16, x1=19, fillcolor='rgba(242,169,59,0.08)', line_width=0,
                           annotation_text="Rush Sore", annotation_font=dict(size=10, color=ACCENT))
            fig2.update_layout(
                title=f'Pola Per Jam — {city_hourly}',
                xaxis=dict(tickmode='array', tickvals=list(range(0,24,2)),
                           ticktext=[f"{h:02d}:00" for h in range(0,24,2)]),
                yaxis_title='Kemacetan (%)', height=340, **PLOTLY_LAYOUT_EXTRA
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("⏳ Menunggu data hourly pattern dari Spark processor...")

        # Estimasi dampak harian
        st.markdown("#### 💰 Estimasi Dampak Kuantitatif Harian")
        df_daily = load_daily_summary(city_filter_t2)
        if not df_daily.empty:
            show_cols = ['summary_date', 'city', 'avg_congestion_index', 'avg_speed',
                         'est_extra_fuel_cost_idr', 'est_extra_travel_min']
            existing = [c for c in show_cols if c in df_daily.columns]
            st.dataframe(
                df_daily[existing].rename(columns={
                    'summary_date':'Tanggal', 'city':'Kota',
                    'avg_congestion_index':'Avg Kemacetan (%)',
                    'avg_speed':'Avg Kecepatan (km/h)',
                    'est_extra_fuel_cost_idr':'Extra Biaya BBM (Rp/hari)',
                    'est_extra_travel_min':'Extra Waktu (menit/hari)',
                }), use_container_width=True, hide_index=True
            )
            st.caption("Asumsi: 30 km/hari, efisiensi 12 km/liter, harga Rp16.250/liter.")
        else:
            st.info("⏳ Menunggu gold_daily_summary dari Spark processor.")
    else:
        st.info("⏳ Menunggu data comparison... Spark processor sedang mengolah.")

    # Tren historis
    st.divider()
    st.markdown("#### 📈 Tren Kemacetan Historis")
    city_hist = city_filter_t2 if city_filter_t2 != "Semua" else None
    df_hist   = load_traffic_history(city_hist, limit=2000)
    if not df_hist.empty:
        df_hist['hour'] = df_hist['recorded_at'].dt.floor('h')
        df_agg = df_hist.groupby(['hour','city'])['congestion_index'].mean().reset_index()
        fig3 = px.line(df_agg, x='hour', y='congestion_index', color='city',
                       title='Tren Kemacetan (Silver / Bronze Layer)',
                       labels={'congestion_index':'Indeks Kemacetan (%)','hour':'Waktu','city':'Kota'},
                       height=320,
                       color_discrete_map={'Jakarta':PLOTLY_AFTER,'Surabaya':PLOTLY_BEFORE,'Yogyakarta':'#2ECC71'})
        fig3.update_layout(**PLOTLY_LAYOUT_EXTRA)
        if df_agg['hour'].min() <= BBM_DT <= df_agg['hour'].max():
            fig3.add_vline(x=BBM_UNIX, line_dash="dash", line_color=ACCENT, line_width=2,
                           annotation_text="⬆ BBM Naik 10 Jun",
                           annotation_font=dict(color=ACCENT, size=11))
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("⏳ Menunggu data Silver/Bronze terkumpul.")

# ══════════════════════════════════════════════════════
# TAB 3 — ML & ANALITIK LANJUTAN
# ══════════════════════════════════════════════════════
with tab3:
    section_header("🤖 ML & Analitik Lanjutan",
                   "Random Forest · KMeans Clustering · Anomaly Detection (Z-Score)")

    # Dropdown kota global untuk tab ini
    city_ml = st.selectbox("🏙️ Filter Kota — Tab ML", ["Semua"] + CITIES_LIST, key="city_ml",
        index=(["Semua"] + CITIES_LIST).index(selected_city) if selected_city in ["Semua"] + CITIES_LIST else 0)
    cities_ml = CITIES_LIST if city_ml == "Semua" else [city_ml]

    st.divider()

    # ── 1. Forecasting ──────────────────────────────────
    st.markdown("##### 1️⃣ Forecasting Kemacetan (Random Forest)")
    st.caption("Prediksi level kemacetan 30 & 60 menit ke depan berdasarkan pola historis")

    predictions = load_predictions()
    if predictions:
        cols = st.columns(len(cities_ml))
        for i, city in enumerate(cities_ml):
            pred = predictions.get(city, {})
            with cols[i]:
                p30 = pred.get('predicted_30m', 0)
                p60 = pred.get('predicted_60m', 0)
                spd = pred.get('current_avg_speed', 0)
                st.markdown(f"""
                <div class="metric-card" style="border-left:3px solid {LEVEL_COLORS[p30]}">
                    <div class="metric-label">{city}</div>
                    <div style="color:#9aa0a6;font-size:11px;margin-bottom:10px">Kecepatan saat ini: <b style="color:#e7ebf5">{spd:.1f} km/h</b></div>
                    <div style="display:flex;gap:8px">
                        <div style="flex:1;background:#0d1117;border:1px solid {LEVEL_COLORS[p30]}33;border-radius:8px;padding:10px;text-align:center">
                            <div style="color:#6b7280;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px">30 Menit</div>
                            <div style="color:{LEVEL_COLORS[p30]};font-size:15px;font-weight:700;margin-top:4px">{LEVEL_LABELS[p30]}</div>
                        </div>
                        <div style="flex:1;background:#0d1117;border:1px solid {LEVEL_COLORS[p60]}33;border-radius:8px;padding:10px;text-align:center">
                            <div style="color:#6b7280;font-size:9px;font-weight:600;text-transform:uppercase;letter-spacing:1px">60 Menit</div>
                            <div style="color:{LEVEL_COLORS[p60]};font-size:15px;font-weight:700;margin-top:4px">{LEVEL_LABELS[p60]}</div>
                        </div>
                    </div>
                </div>""", unsafe_allow_html=True)
    else:
        st.info("⏳ Model forecasting sedang dilatih... Butuh minimal 50 data points.")

    # ── 2. Model Metrics ─────────────────────────────────
    st.divider()
    st.markdown("##### 2️⃣ Evaluasi Model")
    metrics = load_model_metrics()
    if metrics:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Accuracy",          f"{metrics.get('accuracy',0)*100:.1f}%")
        c2.metric("F1-Score (weighted)",f"{metrics.get('f1_score',0)*100:.1f}%")
        c3.metric("Train rows",         f"{metrics.get('train_rows',0):,}")
        c4.metric("Test rows",          f"{metrics.get('test_rows',0):,}")
        st.caption(f"Model: {metrics.get('model','Random Forest')} · Train/Test split 80/20 · "
                   f"Dievaluasi: {metrics.get('evaluated_at','-')}")
    else:
        st.info("⏳ Menunggu hasil evaluasi model dari Spark processor.")

    # ── 3. Clustering (user-friendly) ────────────────────
    st.divider()
    st.markdown("##### 3️⃣ Clustering Zona Dampak BBM (KMeans)")
    st.caption("Ruas jalan dikelompokkan berdasarkan besar kenaikan kemacetan setelah BBM naik")

    clusters = load_clusters(city_ml if city_ml != "Semua" else None)
    if not clusters.empty:
        # Penjelasan zona dalam bahasa sederhana
        col_leg1, col_leg2, col_leg3 = st.columns(3)
        with col_leg1:
            st.markdown("""
            <div class="zone-card" style="background:#2d0a0a;border-color:#E74C3C">
                <div class="zone-title" style="color:#E74C3C">🔴 Zona Merah — Dampak Tinggi</div>
                <div class="zone-desc">Kemacetan naik drastis (&gt;15 pp) setelah BBM naik.
                Ruas ini paling sensitif terhadap perubahan harga BBM. Hindari jam sibuk.</div>
            </div>""", unsafe_allow_html=True)
        with col_leg2:
            st.markdown("""
            <div class="zone-card" style="background:#2d2000;border-color:#F1C40F">
                <div class="zone-title" style="color:#F1C40F">🟡 Zona Kuning — Dampak Sedang</div>
                <div class="zone-desc">Kemacetan naik moderat (5–15 pp). Dampak ada tapi
                masih dalam batas wajar. Pantau terus terutama jam sibuk.</div>
            </div>""", unsafe_allow_html=True)
        with col_leg3:
            st.markdown("""
            <div class="zone-card" style="background:#0a2d0a;border-color:#2ECC71">
                <div class="zone-title" style="color:#2ECC71">🟢 Zona Hijau — Dampak Rendah</div>
                <div class="zone-desc">Kemacetan stabil atau hanya naik sedikit (&lt;5 pp).
                Ruas ini relatif tidak terpengaruh kenaikan BBM.</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Bar chart clustering
        fig_cl = px.bar(
            clusters.sort_values('delta_congestion'),
            x='road_name', y='delta_congestion',
            color='zone_label', color_discrete_map=ZONE_COLORS,
            facet_col='city' if city_ml == "Semua" else None,
            labels={'delta_congestion':'Kenaikan Kemacetan (pp)', 'road_name':'Ruas Jalan', 'zone_label':'Zona'},
            title='Kenaikan Kemacetan per Ruas Jalan Setelah BBM Naik',
            height=400,
        )
        fig_cl.update_layout(**PLOTLY_LAYOUT_EXTRA)
        fig_cl.update_xaxes(tickangle=35)
        if city_ml == "Semua":
            fig_cl.update_xaxes(matches=None)
        st.plotly_chart(fig_cl, use_container_width=True)

        # Peta clustering
        st.markdown("**🗺️ Peta Zona Dampak BBM per Ruas Jalan**")
        center_c = CITY_COORDS[city_ml] if city_ml != "Semua" else {'lat':-7.0,'lon':109.5}
        zoom_c   = 12 if city_ml != "Semua" else 6
        mc = folium.Map(location=[center_c['lat'], center_c['lon']],
                        zoom_start=zoom_c, tiles='OpenStreetMap')

        ROAD_COORDS_ALL = {
            'Jl. Sudirman':(-6.2088,106.8175),'Jl. Thamrin':(-6.1944,106.8229),
            'Jl. HR Rasuna Said':(-6.2258,106.8317),'Jl. Gatot Subroto':(-6.2335,106.8007),
            'Jl. TB Simatupang':(-6.2897,106.7753),'Jl. Ahmad Yani':(-7.3048,112.7373),
            'Jl. Basuki Rahmat':(-7.2659,112.7469),'Jl. Raya Darmo':(-7.2820,112.7313),
            'Jl. Pemuda':(-7.2575,112.7521),'Jl. MERR':(-7.2897,112.7897),
            'Jl. Malioboro':(-7.7925,110.3663),'Jl. Solo':(-7.7833,110.4166),
            'Jl. Magelang':(-7.7614,110.3631),'Ring Road Utara':(-7.7614,110.3897),
            'Jl. Parangtritis':(-7.8319,110.3631),
        }
        for _, row in clusters.iterrows():
            coords = ROAD_COORDS_ALL.get(row['road_name'])
            if not coords: continue
            color = ZONE_COLORS.get(row['zone_label'], '#gray')
            folium.CircleMarker(
                location=list(coords), radius=12,
                color=color, fill=True, fill_opacity=0.85,
                popup=folium.Popup(
                    f"<b style='color:#111'>{row['road_name']}</b><br>"
                    f"Kota: {row['city']}<br>"
                    f"Zona: <b>{row['zone_label']}</b><br>"
                    f"CI Before: {row.get('ci_before',0):.1f}%<br>"
                    f"CI After: {row.get('ci_after',0):.1f}%<br>"
                    f"Kenaikan: +{row.get('delta_congestion',0):.1f} pp",
                    max_width=220
                )
            ).add_to(mc)
        st_folium(mc, width=None, height=320, returned_objects=[])

        # Tabel ringkasan
        st.markdown("**📋 Tabel Ringkasan Clustering**")
        show_cols = [c for c in ['city','road_name','ci_before','ci_after','delta_congestion','zone_label']
                     if c in clusters.columns]
        st.dataframe(
            clusters[show_cols].sort_values('delta_congestion', ascending=False).rename(columns={
                'city':'Kota','road_name':'Ruas Jalan','ci_before':'CI Before (%)',
                'ci_after':'CI After (%)','delta_congestion':'Δ Kemacetan (pp)','zone_label':'Zona Dampak'
            }),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("⏳ Menunggu hasil clustering dari Spark processor.")

    # ── 4. Anomaly Detection ──────────────────────────────
    st.divider()
    st.markdown("##### 4️⃣ Anomaly Detection (Z-Score)")
    st.caption("Titik kemacetan dengan |z-score| > 2 dianggap anomali — lonjakan tidak wajar di luar pola normal")

    anomaly_summary = load_anomaly_summary()
    if anomaly_summary:
        c1, c2, c3 = st.columns(3)
        c1.metric("Tingkat Anomali — Before", f"{anomaly_summary.get('rate_before_pct',0):.2f}%")
        c2.metric("Tingkat Anomali — After",  f"{anomaly_summary.get('rate_after_pct',0):.2f}%",
                  delta=f"{anomaly_summary.get('rate_after_pct',0)-anomaly_summary.get('rate_before_pct',0):.2f} pp")
        c3.metric("Jumlah Anomali (After)",   f"{anomaly_summary.get('anomaly_count_after',0):,}")

        fig_anom = go.Figure(go.Bar(
            x=['Before BBM Naik','After BBM Naik'],
            y=[anomaly_summary.get('rate_before_pct',0), anomaly_summary.get('rate_after_pct',0)],
            marker_color=[PLOTLY_BEFORE, PLOTLY_AFTER],
            text=[f"{anomaly_summary.get('rate_before_pct',0):.2f}%",
                  f"{anomaly_summary.get('rate_after_pct',0):.2f}%"],
            textposition='outside'
        ))
        fig_anom.update_layout(height=280, yaxis_title='Tingkat Anomali (%)',
                               title='Tingkat Anomali: Before vs After BBM', **PLOTLY_LAYOUT_EXTRA)
        st.plotly_chart(fig_anom, use_container_width=True)

        df_anom = load_anomaly_events(city=city_ml if city_ml != "Semua" else None, limit=20)
        if not df_anom.empty:
            st.markdown("**Top anomali terbesar:**")
            st.dataframe(df_anom.rename(columns={
                'city':'Kota','road_name':'Ruas Jalan','recorded_at':'Waktu',
                'congestion_index':'Kemacetan (%)','zscore':'Z-Score'
            }), use_container_width=True, hide_index=True)
    else:
        st.info("⏳ Menunggu hasil anomaly detection dari Spark processor.")

# ══════════════════════════════════════════════════════
# TAB 4 — HARGA BBM
# ══════════════════════════════════════════════════════
with tab4:
    section_header("⛽ Informasi Harga BBM")

    bbm_data = [
        {'Jenis BBM':'Pertamax RON 92','Sebelum':'Rp 12.300/L','Sesudah':'Rp 16.250/L','Kenaikan':'+Rp 3.950 (+32%)'},
        {'Jenis BBM':'Pertamax Green 95','Sebelum':'Rp 12.900/L','Sesudah':'Rp 17.000/L','Kenaikan':'+Rp 4.100 (+32%)'},
        {'Jenis BBM':'Pertalite RON 90','Sebelum':'Rp 10.000/L','Sesudah':'Rp 10.000/L','Kenaikan':'Tidak naik'},
    ]
    st.dataframe(pd.DataFrame(bbm_data), use_container_width=True, hide_index=True)
    st.markdown("""
    <div class="bbm-card">
        <b>📅 Tanggal Berlaku:</b> 10 Juni 2026<br>
        <b>📌 Sumber:</b> Pertamina Official (CNBC Indonesia)<br>
        <b>💡 Dampak:</b> Kenaikan ~32% memicu perubahan pola mobilitas di kota besar
    </div>""", unsafe_allow_html=True)

    fig_bbm = go.Figure()
    fuels   = ['Pertamax RON 92','Pertamax Green 95','Pertalite RON 90']
    before_ = [12300, 12900, 10000]
    after_  = [16250, 17000, 10000]
    fig_bbm.add_trace(go.Bar(name='Sebelum 10 Juni 2026', x=fuels, y=before_,
                             marker_color=PLOTLY_BEFORE, text=[f"Rp{v:,}" for v in before_],
                             textposition='outside'))
    fig_bbm.add_trace(go.Bar(name='Sesudah 10 Juni 2026', x=fuels, y=after_,
                             marker_color=PLOTLY_AFTER, text=[f"Rp{v:,}" for v in after_],
                             textposition='outside'))
    fig_bbm.update_layout(title='Perbandingan Harga BBM (per liter)',
                          barmode='group', yaxis_title='Harga (Rp)',
                          height=360, **PLOTLY_LAYOUT_EXTRA)
    st.plotly_chart(fig_bbm, use_container_width=True)

    st.markdown("#### 💸 Kalkulator Dampak Kenaikan BBM")
    col1, col2 = st.columns(2)
    with col1:
        konsumsi = st.slider("Konsumsi BBM per hari (liter)", 1, 20, 5)
        jenis    = st.selectbox("Jenis BBM", ['Pertamax RON 92','Pertamax Green 95','Pertalite RON 90'])
    harga_map = {'Pertamax RON 92':(12300,16250),'Pertamax Green 95':(12900,17000),'Pertalite RON 90':(10000,10000)}
    hb, ha    = harga_map[jenis]
    sel_hari  = (ha - hb) * konsumsi
    sel_bln   = sel_hari * 30
    sel_thn   = sel_hari * 365
    with col2:
        metric_card("Biaya BBM Harian (Sesudah)", f"Rp {ha*konsumsi:,}",
                    f"Sebelumnya Rp {hb*konsumsi:,} · Selisih +Rp {sel_hari:,}/hari",
                    value_color="#E74C3C" if sel_hari > 0 else "#2ECC71")
        metric_card("Tambahan Pengeluaran / Bulan", f"Rp {sel_bln:,}",
                    f"Per tahun: Rp {sel_thn:,}",
                    value_color=ACCENT if sel_hari > 0 else "#2ECC71")

# ══════════════════════════════════════════════════════
# TAB 5 — ARSITEKTUR & LAKEHOUSE
# ══════════════════════════════════════════════════════
with tab5:
    section_header("🏗️ Arsitektur Big Data & Medallion Lakehouse")
    st.markdown("#### 📐 Pipeline End-to-End")
    st.code(
        "TomTom Traffic API (Real-Time)\n"
        "        │\n"
        "        ▼\n"
        "  collector (Kafka Producer) ──► Kafka topics: traffic.raw\n"
        "        │                              │\n"
        "        │                              ▼\n"
        "        │                    bronze_consumer (Kafka Consumer)\n"
        "        │                              │\n"
        "        ▼                              ▼\n"
        "  Redis (fast-path cache)   BRONZE: Postgres + Parquet\n"
        "  └─ Peta Real-Time                    │\n"
        "        │                              ▼\n"
        "        │                 spark_processor (PySpark, tiap 5 menit)\n"
        "        │                  ┌────────────┼────────────┐\n"
        "        │                  ▼            ▼            ▼\n"
        "        │            SILVER layer   GOLD layer   ML Models\n"
        "        │            (cleaned)   (aggregated)  RF + KMeans\n"
        "        └─────────────────────────────────────────────┘\n"
        "                                       ▼\n"
        "              Streamlit Dashboard + OpenStreetMap + OSRM Routing",
        language=None
    )

    st.markdown("#### 🧱 Status Medallion Lakehouse (Live)")
    counts = load_layer_counts()
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown('<span class="layer-badge badge-bronze">BRONZE</span> raw ingestion', unsafe_allow_html=True)
        st.metric("bronze_traffic", f"{counts.get('bronze_traffic') or 0:,}")
    with c2:
        st.markdown('<span class="layer-badge badge-silver">SILVER</span> cleaned + features', unsafe_allow_html=True)
        st.metric("silver_traffic", f"{counts.get('silver_traffic') or 0:,}")
    with c3:
        st.markdown('<span class="layer-badge badge-gold">GOLD</span> agregat & ML output', unsafe_allow_html=True)
        st.metric("gold_city_comparison", f"{counts.get('gold_city_comparison') or 0:,}")
        st.metric("gold_road_clusters / gold_anomalies",
                  f"{counts.get('gold_road_clusters') or 0:,} / {counts.get('gold_anomalies') or 0:,}")

    st.divider()
    st.markdown("#### 🔢 Kerangka 5V Big Data")
    v_data = [
        {"V":"Volume", "Penjelasan":f"~{(counts.get('bronze_traffic') or 0):,} baris traffic dari 15 titik jalan × 3 kota, tumbuh tiap 60 detik dari live collector + batch historis 1 Mei–19 Jun 2026."},
        {"V":"Velocity","Penjelasan":"Ingestion real-time tiap 60 detik (TomTom→Kafka), diproses Spark tiap 5 menit (near-real-time)."},
        {"V":"Variety", "Penjelasan":"Data traffic API (TomTom, JSON) + data harga BBM resmi Pertamina + data historis simulasi berbasis pola realistis."},
        {"V":"Veracity","Penjelasan":"Data live dari API resmi TomTom; data historis memakai simulasi berbasis pola jam sibuk/weekend yang terdokumentasi."},
        {"V":"Value",   "Penjelasan":"Dashboard kebijakan (efektivitas BBM→mobilitas), zona terdampak (clustering), deteksi lonjakan (anomaly), estimasi kerugian ekonomi."},
    ]
    st.dataframe(pd.DataFrame(v_data), use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("#### ⚖️ Analisis Kompetitor")
    comp_data = [
        {"Solusi":"MyPertamina","Real-time traffic":"❌","Prediksi kemacetan":"❌","Korelasi BBM↔Mobilitas":"❌","Catatan":"Hanya harga statis"},
        {"Solusi":"Google Maps","Real-time traffic":"✅","Prediksi kemacetan":"✅ (ETA)","Korelasi BBM↔Mobilitas":"❌","Catatan":"Navigasi umum, tanpa analisis kebijakan"},
        {"Solusi":"FuelWatch Indonesia","Real-time traffic":"✅","Prediksi kemacetan":"✅ (RF 30/60m)","Korelasi BBM↔Mobilitas":"✅ (before/after, clustering, anomaly)","Catatan":"Khusus analisis dampak kebijakan BBM"},
    ]
    st.dataframe(pd.DataFrame(comp_data), use_container_width=True, hide_index=True)

# ── Auto Refresh ────────────────────────────────────────────
if auto_refresh:
    import time
    time.sleep(60)
    st.rerun()
