import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

st.set_page_config(
    page_title="AirWatch MX — Monitor de Calidad del Aire",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS Personalizado ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=DM+Sans:wght@300;400;500;600&display=swap');

/* Reset y base */
html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

/* Fondo principal */
.stApp {
    background-color: #0a0e1a;
    color: #e2e8f0;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #0d1220;
    border-right: 1px solid #1e2d45;
}

[data-testid="stSidebar"] * {
    color: #94a3b8 !important;
}

/* Título principal */
h1 {
    font-family: 'Space Mono', monospace !important;
    font-size: 2rem !important;
    color: #38bdf8 !important;
    letter-spacing: -0.02em;
    margin-bottom: 0 !important;
}

h2, h3 {
    font-family: 'DM Sans', sans-serif !important;
    color: #cbd5e1 !important;
    font-weight: 500 !important;
}

/* Cards de métricas */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #0f1f35 0%, #0d1829 100%);
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    padding: 20px !important;
    transition: border-color 0.2s ease;
}

[data-testid="metric-container"]:hover {
    border-color: #38bdf8;
}

[data-testid="stMetricLabel"] {
    color: #64748b !important;
    font-size: 0.75rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}

[data-testid="stMetricValue"] {
    color: #38bdf8 !important;
    font-family: 'Space Mono', monospace !important;
    font-size: 2.2rem !important;
}

/* Selectbox */
[data-testid="stSelectbox"] > div > div {
    background-color: #0f1f35 !important;
    border: 1px solid #1e3a5f !important;
    border-radius: 8px !important;
    color: #e2e8f0 !important;
}

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid #1e3a5f;
    border-radius: 12px;
    overflow: hidden;
}

/* Divider */
hr {
    border-color: #1e2d45 !important;
    margin: 1.5rem 0 !important;
}

/* Badge tag */
.tag {
    display: inline-block;
    background: #0c2a4a;
    color: #38bdf8;
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    padding: 3px 10px;
    border-radius: 20px;
    border: 1px solid #1e3a5f;
    margin-right: 6px;
}

.subtitle {
    color: #475569;
    font-size: 0.9rem;
    margin-top: 4px;
}

.section-header {
    color: #64748b;
    font-family: 'Space Mono', monospace;
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-bottom: 12px;
}
</style>
""", unsafe_allow_html=True)


# --- Conexión DB ---
@st.cache_resource
def get_engine():
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    db       = os.getenv("DB_NAME", "air_quality")
    user     = os.getenv("DB_USER", "pipeline_user")
    password = os.getenv("DB_PASSWORD", "pipeline123")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True
    )


@st.cache_data(ttl=3600)
def load_locations():
    engine = get_engine()
    query = text("""
        SELECT location_id, name, locality, country,
               latitude, longitude, sensors_count, updated_at
        FROM locations ORDER BY sensors_count DESC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


# --- Plotly theme oscuro ---
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="DM Sans", color="#94a3b8"),
    margin=dict(l=10, r=10, t=10, b=10),
)

# --- Sidebar ---
with st.sidebar:
    st.markdown("""
    <div style='padding: 8px 0 24px 0'>
        <div style='font-family: Space Mono, monospace; font-size: 1.1rem; 
                    color: #38bdf8; font-weight: 700;'>AIRWATCH MX</div>
        <div style='color: #334155; font-size: 0.75rem; margin-top: 2px;'>
            v1.0 · Data Engineering Portfolio
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-header">Filtros</div>', unsafe_allow_html=True)

    df = load_locations()
    localidades = ["Todas"] + sorted(df["locality"].dropna().unique().tolist())
    selected_locality = st.selectbox("Localidad", localidades, label_visibility="collapsed")

    if selected_locality != "Todas":
        df_filtered = df[df["locality"] == selected_locality]
    else:
        df_filtered = df.copy()

    st.markdown("---")
    st.markdown('<div class="section-header">Pipeline Status</div>', unsafe_allow_html=True)

    if not df.empty:
        last_update = pd.to_datetime(df["updated_at"]).max()
        st.markdown(f"""
        <div style='background:#0a2a1a; border:1px solid #166534; border-radius:8px; 
                    padding:10px 14px; margin-bottom:12px;'>
            <div style='color:#4ade80; font-size:0.7rem; font-family:Space Mono,monospace;
                        text-transform:uppercase; letter-spacing:0.08em;'>● ACTIVO</div>
            <div style='color:#64748b; font-size:0.72rem; margin-top:4px;'>
                Última ejecución:<br>
                <span style='color:#94a3b8; font-family:Space Mono,monospace; font-size:0.7rem;'>
                {last_update.strftime('%Y-%m-%d %H:%M')} UTC
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
    <div style='margin-top: 8px;'>
        <div class="section-header">Stack Técnico</div>
        <span class="tag">Airflow</span>
        <span class="tag">PostgreSQL</span><br><br>
        <span class="tag">OpenAQ API</span>
        <span class="tag">Python</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown(
        "<a href='https://github.com/EmilianoMAl/air-quality-pipeline' "
        "style='color:#38bdf8; font-size:0.8rem; text-decoration:none;'>"
        "↗ Ver código en GitHub</a>",
        unsafe_allow_html=True
    )


# --- Header ---
st.markdown("""
<div style='margin-bottom: 8px;'>
    <h1>🌬️ AirWatch MX</h1>
    <p class='subtitle'>
        Pipeline ETL en tiempo real · OpenAQ API → Apache Airflow → PostgreSQL → Streamlit
    </p>
</div>
""", unsafe_allow_html=True)
st.markdown("---")

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Estaciones monitoreadas", len(df))
with col2:
    st.metric("Localidades activas", df["locality"].nunique())
with col3:
    st.metric("Sensores en red", int(df["sensors_count"].sum()))
with col4:
    top_locality = df.groupby("locality")["sensors_count"].sum().idxmax() if not df.empty else "—"
    st.metric("Mayor cobertura", top_locality)

st.markdown("---")

# --- Mapa + Tabla ---
col_map, col_table = st.columns([3, 2])

with col_map:
    st.markdown('<div class="section-header">Distribución Geográfica</div>', unsafe_allow_html=True)
    if not df_filtered.empty:
        fig_map = px.scatter_mapbox(
            df_filtered,
            lat="latitude",
            lon="longitude",
            hover_name="name",
            hover_data={"locality": True, "sensors_count": True,
                        "latitude": False, "longitude": False},
            color="sensors_count",
            color_continuous_scale=[[0, "#0c2a4a"], [0.5, "#0284c7"], [1, "#38bdf8"]],
            size="sensors_count",
            size_max=22,
            zoom=4,
            mapbox_style="carto-darkmatter",
            labels={"sensors_count": "Sensores"},
        )
        fig_map.update_layout(
            **PLOTLY_LAYOUT,
            height=420,
            coloraxis_colorbar=dict(
                title=dict(text="Sensores", font=dict(color="#64748b")),
                tickfont=dict(color="#64748b"),
            )
        )
        st.plotly_chart(fig_map, use_container_width=True)

with col_table:
    st.markdown('<div class="section-header">Detalle de Estaciones</div>', unsafe_allow_html=True)
    st.dataframe(
        df_filtered[["name", "locality", "sensors_count"]].rename(columns={
            "name": "Estación",
            "locality": "Localidad",
            "sensors_count": "Sensores"
        }),
        use_container_width=True,
        height=420,
        hide_index=True,
    )

st.markdown("---")

# --- Gráfica barras ---
st.markdown('<div class="section-header">Cobertura por Localidad</div>', unsafe_allow_html=True)

df_grouped = (
    df_filtered
    .groupby("locality", dropna=False)["sensors_count"]
    .sum()
    .reset_index()
    .rename(columns={"locality": "Localidad", "sensors_count": "Sensores"})
    .sort_values("Sensores", ascending=True)
)

if not df_grouped.empty:
    fig_bar = go.Figure(go.Bar(
        x=df_grouped["Sensores"],
        y=df_grouped["Localidad"],
        orientation="h",
        marker=dict(
            color=df_grouped["Sensores"],
            colorscale=[[0, "#0c2a4a"], [0.5, "#0284c7"], [1, "#38bdf8"]],
            line=dict(width=0),
        ),
        text=df_grouped["Sensores"],
        textposition="outside",
        textfont=dict(color="#64748b", size=11),
    ))
    fig_bar.update_layout(
        **PLOTLY_LAYOUT,
        height=380,
        xaxis=dict(showgrid=True, gridcolor="#1e2d45", color="#475569"),
        yaxis=dict(showgrid=False, color="#94a3b8"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("---")
st.markdown(
    "<div style='color:#334155; font-size:0.75rem; font-family:Space Mono,monospace;'>"
    "AIRWATCH MX · Datos: OpenAQ API · Orquestación: Apache Airflow · "
    "Almacenamiento: PostgreSQL · Portfolio — Emiliano"
    "</div>",
    unsafe_allow_html=True
)