from __future__ import annotations

import hashlib
import json
import os
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time
from typing import Any

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from backend_config import resolve_dashboard_backend
from refresh_metadata import (
    format_refresh_timestamp,
    latest_source_extraction_timestamp,
)
from snowflake_backend import REQUIRED_QUALITY_METRICS, load_snowflake_dashboard_data
from streamlit.errors import StreamlitSecretNotFoundError

FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64" viewBox="0 0 24 24">'
    '<path d="M3.5 21V5.5L13 2v19M13 9h7.5v12" fill="none" '
    'stroke="#21B6E8" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>'
    '<path d="M6.5 8h2M6.5 11.5h2M6.5 15h2M16 12h2M16 15.5h2" fill="none" '
    'stroke="#21B6E8" stroke-width="1.5" stroke-linecap="round"/>'
    "</svg>"
)

st.set_page_config(
    page_title="Toronto Housing Bottleneck Analytics",
    page_icon=FAVICON_SVG,
    layout="wide",
    initial_sidebar_state="expanded",
)

# The mutable warehouse-latest release is published by CD after an eligible
# CI run succeeds; CD runs ingestion and dbt build/tests before publication.
# Download the published database and query curated dbt relations read-only.
# No local project database is required.
DUCKDB_RELEASE_URL = (
    "https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/"
    "releases/download/warehouse-latest/toronto.duckdb"
)
RELEASE_PAGE_URL = (
    "https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/"
    "releases/tag/warehouse-latest"
)
METRICFLOW_RELEASE_URL = (
    "https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/"
    "releases/download/metrics-latest/toronto-metricflow-results.json"
)
DBT_SCHEMA = "analytics"
CACHE_TTL_SECONDS = 24 * 60 * 60
NAV = [
    "The story",
    "Active permits",
    "Closed-permit timelines",
    "Data quality & definitions",
    "Metrics & semantic layer",
]


# The dashboard uses one Light theme, configured in .streamlit/config.toml.
# Do not infer the palette from st.context.theme during initial page load.
THEME_TYPE = "light"  # Palette for Python-rendered Plotly figures
if THEME_TYPE == "dark":
    NAVY = "#e8eef7"
    TEAL = "#45c4c8"
    BLUE = "#7ca8df"
    GOLD = "#f0bd62"
    RED = "#ff8178"
    MUTED = "#adbbce"
    PAGE_BG = "#0b1220"
    SIDEBAR_BG = "#111b2b"
    PANEL_BG = "#192538"
    PLOT_BG = "#121d2d"
    TEXT_COLOR = "#e8eef7"
    GRID_COLOR = "#35445a"
    BORDER_COLOR = "#33445a"
    LINK_COLOR = "#5ce1e6"
    HOVER_BG = "#192538"
    HOVER_TEXT = "#e8eef7"
    SCALE_LOW = "#294758"
    PLOTLY_TEMPLATE = "plotly_dark"
else:
    NAVY = "#28343E"
    TEAL = "#21B6E8"
    BLUE = "#168DB8"
    GOLD = "#4F91A8"
    RED = "#D96B68"
    MUTED = "#687783"
    PAGE_BG = "#E2E7ED"
    SIDEBAR_BG = "#303B46"
    PANEL_BG = "#FFFFFF"
    PLOT_BG = "#FFFFFF"
    TEXT_COLOR = "#28343E"
    GRID_COLOR = "#E1E7ED"
    BORDER_COLOR = "#DCE3E9"
    LINK_COLOR = "#21B6E8"
    HOVER_BG = "#FFFFFF"
    HOVER_TEXT = "#28343E"
    SCALE_LOW = "#BFE8F4"
    PLOTLY_TEMPLATE = "plotly_white"

st.markdown(
    f"""
<style>
:root {{
  --app-sidebar-bg: {SIDEBAR_BG};
  --app-sidebar-text: #F4F7FA;
  --app-sidebar-muted: #C0CAD3;
  --app-sidebar-link: #28B8E8;
}}
[data-testid="stAppViewContainer"] {{
  --app-page-bg: var(--background-color, {PAGE_BG});
  --app-panel-bg: var(--secondary-background-color, {PANEL_BG});
  --app-text: var(--text-color, {TEXT_COLOR});
  --app-muted: color-mix(in srgb, var(--app-text) 68%, transparent);
  --app-border: var(--secondary-background-color, {BORDER_COLOR});
  --app-link: var(--primary-color, {LINK_COLOR});
}}
html, body, button, input, textarea, select,
[data-testid="stAppViewContainer"], [data-testid="stSidebar"] {{
  font-family: "Inter", "Segoe UI", "Aptos", "Helvetica Neue", Arial, sans-serif !important;
}}
header[data-testid="stHeader"] {{
  background: transparent !important;
}}
.stDeployButton, .stAppDeployButton {{
  display: none !important;
}}
.block-container {{ padding-top: 3.5rem; padding-bottom: 2rem; max-width: 1600px; }}
[data-testid="stAppViewContainer"] {{ background: var(--app-page-bg); color: var(--app-text); }}
/* Main-pane widget labels follow Streamlit's active Light/Dark theme variables. */
[data-testid="stMain"] [data-testid="stWidgetLabel"],
[data-testid="stMain"] [data-testid="stWidgetLabel"] *,
[data-testid="stMain"] [data-testid="stSlider"] label,
[data-testid="stMain"] [data-testid="stRadio"] label,
[data-testid="stMain"] [data-testid="stRadio"] label *,
[data-testid="stMain"] [data-testid="stCheckbox"] label,
[data-testid="stMain"] [data-testid="stCheckbox"] label *,
[data-testid="stMain"] [data-testid="stSelectbox"] label,
[data-testid="stMain"] [data-testid="stMultiSelect"] label,
[data-testid="stMain"] [data-testid="stNumberInput"] label,
[data-testid="stMain"] [data-testid="stDateInput"] label,
[data-testid="stMain"] [data-testid="stTextInput"] label {{
  color: var(--app-text) !important;
  -webkit-text-fill-color: var(--app-text) !important;
}}
[data-testid="stSidebar"] {{
  background: var(--app-sidebar-bg) !important;
  border-right: 1px solid #46515C;
  color: var(--app-sidebar-text) !important;
}}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"],
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] li,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] [data-testid="stWidgetLabel"] {{
  color: var(--app-sidebar-text) !important;
}}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {{
  color: var(--app-sidebar-muted) !important;
}}
[data-testid="stSidebar"] a {{
  color: var(--app-sidebar-link) !important;
  font-weight: 600;
}}
[data-testid="stSidebar"] [role="radiogroup"] label {{ color: var(--app-sidebar-text) !important; }}
.hero {{
  background: var(--app-panel-bg);
  border: 1px solid var(--app-border);
  padding: 1.35rem 1.8rem 1.45rem;
  border-radius: 15px;
  color: var(--app-text);
  margin-bottom: .8rem;
  text-align: center;
}}
.hero h1 {{
  color: var(--app-text) !important;
  margin: 0 auto .55rem auto;
  font-size: 1.9rem;
  font-weight: 750;
  line-height: 1.22;
  letter-spacing: -.02em;
  text-align: center;
}}
.hero p {{
  color: var(--app-muted) !important;
  margin: 0 auto;
  font-size: 1.02rem;
  line-height: 1.55;
  max-width: 1120px;
  text-align: center;
}}
.eyebrow {{
  text-transform: uppercase;
  letter-spacing: .115em;
  font-size: .88rem;
  line-height: 1.45;
  font-weight: 750;
  color: var(--app-link);
  margin: 0 auto .58rem auto;
  text-align: center;
}}
.section-note {{ color: var(--app-muted); font-size: .94rem; line-height: 1.55; margin-top: -.25rem; margin-bottom: 1rem; }}
.metric-card {{
  border: 1px solid var(--app-border);
  border-top: 3px solid var(--app-link);
  background: var(--app-panel-bg);
  border-radius: 12px;
  padding: 1.1rem 1rem;
  min-height: 132px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  text-align: center;
  box-shadow: 0 3px 12px rgba(38, 52, 62, .055);
}}
.metric-label {{ color: var(--app-muted); font-size: .94rem; line-height: 1.35; font-weight: 650; margin-bottom: .52rem; text-align: center; }}
.metric-value {{ color: var(--app-text); font-size: 2.2rem; line-height: 1.12; font-weight: 800; letter-spacing: -.035em; text-align: center; }}
.metric-foot {{ color: var(--app-muted); font-size: .88rem; line-height: 1.45; margin-top: .55rem; text-align: center; max-width: 100%; }}
.callout {{ border-left: 4px solid {TEAL}; background: var(--app-panel-bg); padding: .9rem 1rem; border-radius: 0 10px 10px 0; color: var(--app-text); }}
.small-muted {{ color: var(--app-muted); font-size: .85rem; }}
div[data-testid="stPlotlyChart"] {{ border: 1px solid var(--app-border); border-radius: 12px; padding: .45rem; background: var(--app-panel-bg); }}
/* Sidebar brand, supporting line and action-control contrast. */
.sidebar-brand {{
  display: flex;
  align-items: center;
  gap: .55rem;
  color: var(--app-sidebar-text, #F4F7FA);
  font-size: 1.08rem;
  font-weight: 750;
  line-height: 1.25;
  margin: .15rem 0 .2rem;
}}
.sidebar-brand svg {{ flex: 0 0 auto; }}
.sidebar-tagline {{
  color: var(--app-sidebar-muted, #C0CAD3) !important;
  font-size: 1rem;
  font-weight: 600;
  line-height: 1.45;
  margin: .2rem 0 .55rem;
}}
.st-key-story_data_context [data-testid="stMarkdownContainer"],
.st-key-story_data_context [data-testid="stCaptionContainer"] {{
  text-align: center !important;
}}
.st-key-story_data_context [data-testid="stMarkdownContainer"] p,
.st-key-story_data_context [data-testid="stCaptionContainer"] p {{
  text-align: center !important;
}}
/* V13: one consistent sidebar/button palette and a visible slider surface. */
/* All sidebar buttons share graphite default/disabled/loading colors. */
[data-testid="stSidebar"] button,
[data-testid="stSidebar"] button:disabled,
[data-testid="stSidebar"] button[disabled],
[data-testid="stSidebar"] .st-key-refresh_data_button button {{
  appearance: none !important;
  background: #303B46 !important;
  background-color: #303B46 !important;
  background-image: none !important;
  color: #F3F6F9 !important;
  -webkit-text-fill-color: #F3F6F9 !important;
  border: 1px solid #687782 !important;
  box-shadow: none !important;
  opacity: 1 !important;
  filter: none !important;
  font-weight: 700 !important;
  transition: background-color .15s ease, color .15s ease, border-color .15s ease;
}}
[data-testid="stSidebar"] button *,
[data-testid="stSidebar"] .st-key-refresh_data_button button * {{
  background: transparent !important;
  background-color: transparent !important;
  color: #F3F6F9 !important;
  -webkit-text-fill-color: #F3F6F9 !important;
  opacity: 1 !important;
}}
[data-testid="stSidebar"] button:hover:not(:disabled),
[data-testid="stSidebar"] .st-key-refresh_data_button button:hover:not(:disabled) {{
  background: #F3F6F9 !important;
  background-color: #F3F6F9 !important;
  background-image: none !important;
  color: #303B46 !important;
  -webkit-text-fill-color: #303B46 !important;
  border-color: #21B6E8 !important;
  box-shadow: 0 0 0 1px rgba(33, 182, 232, .14) !important;
}}
[data-testid="stSidebar"] button:hover:not(:disabled) *,
[data-testid="stSidebar"] .st-key-refresh_data_button button:hover:not(:disabled) * {{
  background: transparent !important;
  background-color: transparent !important;
  color: #303B46 !important;
  -webkit-text-fill-color: #303B46 !important;
}}
[data-testid="stSidebar"] button:focus-visible {{
  outline: 2px solid #21B6E8 !important;
  outline-offset: 2px !important;
}}

/* Fallback styling for any Streamlit status component still used elsewhere. */
[data-testid="stStatusWidget"],
[data-testid="stStatusWidget"] *,
div.stStatusWidget,
div.stStatusWidget * {{
  background-color: #303B46 !important;
  color: #F3F6F9 !important;
  -webkit-text-fill-color: #F3F6F9 !important;
  border-color: #687782 !important;
  opacity: 1 !important;
  filter: none !important;
}}
[data-testid="stStatusWidget"] svg,
div.stStatusWidget svg {{ color: #21B6E8 !important; }}

/* Slightly shaded input/select surfaces. */
div[data-testid="stNumberInput"] [data-baseweb="input"] > div,
div[data-testid="stDateInput"] [data-baseweb="input"] > div,
div[data-testid="stSelectbox"] [data-baseweb="select"] > div,
div[data-testid="stMultiSelect"] [data-baseweb="select"] > div {{
  background-color: var(--app-panel-bg) !important;
  border-color: var(--app-border) !important;
}}
div[data-testid="stNumberInput"] input,
div[data-testid="stDateInput"] input,
div[data-testid="stSelectbox"] [data-baseweb="select"] *,
div[data-testid="stMultiSelect"] [data-baseweb="select"] * {{
  color: var(--app-text) !important;
  -webkit-text-fill-color: var(--app-text) !important;
}}
div[data-testid="stMultiSelect"] [data-baseweb="tag"] {{
  background-color: var(--app-panel-bg) !important;
  color: var(--app-text) !important;
}}

/* Keep slider labels on the page canvas; draw a separate panel behind the control. */
div[data-testid="stSlider"] label,
.stSlider label {{
  background: transparent !important;
  background-color: transparent !important;
}}
div[data-testid="stSlider"],
.stSlider {{
  background: transparent !important;
  background-color: transparent !important;
  border: 0 !important;
  box-shadow: none !important;
}}
div[data-testid="stSlider"] [data-baseweb="slider"],
.stSlider [data-baseweb="slider"] {{
  position: relative !important;
  z-index: 0 !important;
  isolation: isolate !important;
  overflow: visible !important;
  box-sizing: border-box !important;
  padding: .42rem .8rem .35rem !important;
  border-radius: 10px !important;
}}
div[data-testid="stSlider"] [data-baseweb="slider"]::before,
.stSlider [data-baseweb="slider"]::before {{
  content: "" !important;
  position: absolute !important;
  inset: -.25rem -.55rem -.55rem !important;
  background: var(--app-panel-bg) !important;
  background-color: var(--app-panel-bg) !important;
  border: 1px solid var(--app-border) !important;
  border-radius: 10px !important;
  z-index: -1 !important;
  pointer-events: none !important;
}}
</style>
""",
    unsafe_allow_html=True,
)


def download_release_database(destination: Path) -> int:
    """Download the rolling GitHub release asset to a temporary local file."""
    downloaded = 0
    with requests.get(
        DUCKDB_RELEASE_URL,
        stream=True,
        allow_redirects=True,
        timeout=(15, 180),
        headers={"User-Agent": "toronto-housing-bottleneck-streamlit/1.0"},
    ) as response:
        response.raise_for_status()
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/html" in content_type:
            raise RuntimeError(
                "GitHub returned HTML instead of the toronto.duckdb release asset. "
                "Confirm the public release URL is still valid."
            )
        with destination.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)
                    downloaded += len(chunk)
    if downloaded < 1_000_000:
        raise RuntimeError(
            f"The downloaded DuckDB asset is unexpectedly small ({downloaded:,} bytes)."
        )
    return downloaded


def selected_dashboard_backend() -> str:
    """Resolve the explicit backend setting; DuckDB is the default."""
    environment_backend = os.environ.get("DASHBOARD_BACKEND")
    if environment_backend is not None:
        return resolve_dashboard_backend(environment_backend)

    try:
        secrets_backend = st.secrets.get("DASHBOARD_BACKEND", "duckdb")
    except StreamlitSecretNotFoundError:
        secrets_backend = "duckdb"

    return resolve_dashboard_backend(None, secrets_backend)


def snowflake_dashboard_settings() -> dict[str, Any]:
    """Read Snowflake connection settings without exposing them to the UI."""
    try:
        settings = st.secrets["snowflake"]
    except StreamlitSecretNotFoundError as exc:
        raise RuntimeError(
            "Snowflake is selected, but Streamlit secrets are unavailable. "
            "Configure the [snowflake] secrets section before using this backend."
        ) from exc
    except KeyError as exc:
        raise RuntimeError(
            "Snowflake is selected, but the [snowflake] secrets section is missing."
        ) from exc

    return dict(settings)


@st.cache_data(ttl=CACHE_TTL_SECONDS, max_entries=1, show_spinner=False)
def load_dashboard_data(
    backend: str = "duckdb",
    config_fingerprint: str = "",
    duckdb_path: str = "",
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    int,
    dict[str, int],
    Any | None,
    Any | None,
]:
    """Read the same tested dbt serving contract from the selected backend."""
    if backend == "snowflake":
        return load_snowflake_dashboard_data(snowflake_dashboard_settings())
    if backend != "duckdb":
        raise ValueError("Unsupported dashboard backend.")

    with ExitStack() as stack:
        configured_path = duckdb_path.strip()
        if configured_path:
            database_path = Path(configured_path).expanduser()
            if not database_path.is_file():
                raise FileNotFoundError(
                    "DASHBOARD_DUCKDB_PATH does not point to an existing file: "
                    f"{database_path}"
                )
        else:
            temp_dir = stack.enter_context(
                TemporaryDirectory(prefix="toronto-housing-dashboard-")
            )
            database_path = Path(temp_dir) / "toronto.duckdb"
            download_release_database(database_path)

        connection = duckdb.connect(str(database_path), read_only=True)
        try:
            available = {
                (row[0], row[1])
                for row in connection.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables"
                ).fetchall()
            }
            required = {
                (DBT_SCHEMA, "mart_active_permit_age"),
                (DBT_SCHEMA, "mart_active_backlog_summary"),
                (DBT_SCHEMA, "mart_active_backlog_by_year_type"),
                (DBT_SCHEMA, "mart_cleared_lifecycle_by_year_type"),
                (DBT_SCHEMA, "mart_cleared_lifecycle_matched_by_year_type"),
                (DBT_SCHEMA, "mart_cleared_lifecycle_pooled"),
                (DBT_SCHEMA, "mart_data_quality_summary"),
            }
            missing = sorted(required - available)
            if missing:
                missing_names = ", ".join(
                    f"{schema}.{table}" for schema, table in missing
                )
                raise RuntimeError(
                    "The downloaded release does not contain required dbt relations: "
                    f"{missing_names}. The release may not yet contain the new dbt serving contract."
                )

            active = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_active_permit_age"
            ).fetchdf()
            summary = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_cleared_lifecycle_by_year_type"
            ).fetchdf()
            active_backlog = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_active_backlog_summary"
            ).fetchdf()
            active_backlog_by_year_type = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_active_backlog_by_year_type"
            ).fetchdf()
            matched_lifecycle = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_cleared_lifecycle_matched_by_year_type"
            ).fetchdf()
            pooled_lifecycle = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_cleared_lifecycle_pooled"
            ).fetchdf()
            quality_frame = connection.execute(
                f"SELECT metric_name, metric_value FROM {DBT_SCHEMA}.mart_data_quality_summary"
            ).fetchdf()

            last_successful_refresh_utc = None
            if ("metadata", "dashboard_refresh_metadata") in available:
                refresh_row = connection.execute(
                    "SELECT last_successful_refresh_utc "
                    "FROM metadata.dashboard_refresh_metadata WHERE refresh_id = 1"
                ).fetchone()
                if refresh_row:
                    last_successful_refresh_utc = refresh_row[0]
            last_successful_source_extraction_utc = latest_source_extraction_timestamp(
                connection, available
            )
        finally:
            connection.close()

    for label, frame in (
        ("mart_active_permit_age", active),
        ("mart_cleared_lifecycle_by_year_type", summary),
        ("mart_active_backlog_summary", active_backlog),
        ("mart_active_backlog_by_year_type", active_backlog_by_year_type),
        ("mart_cleared_lifecycle_matched_by_year_type", matched_lifecycle),
        ("mart_cleared_lifecycle_pooled", pooled_lifecycle),
        ("mart_data_quality_summary", quality_frame),
    ):
        if frame.empty:
            raise RuntimeError(f"analytics.{label} returned no records.")

    quality_counts = {
        str(row.metric_name): int(row.metric_value or 0)
        for row in quality_frame.itertuples(index=False)
    }
    missing_metrics = sorted(set(REQUIRED_QUALITY_METRICS) - set(quality_counts))
    if missing_metrics:
        raise RuntimeError(
            "analytics.mart_data_quality_summary is missing metrics: "
            + ", ".join(missing_metrics)
        )
    cleared_revision_count = quality_counts["cleared_revision_count"]
    return (
        active,
        summary,
        active_backlog,
        active_backlog_by_year_type,
        matched_lifecycle,
        pooled_lifecycle,
        cleared_revision_count,
        quality_counts,
        last_successful_refresh_utc,
        last_successful_source_extraction_utc,
    )


def normalize_bool(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(
            {
                "true": True,
                "1": True,
                "yes": True,
                "false": False,
                "0": False,
                "no": False,
            }
        )
        .fillna(False)
    )


def number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def fmt_int(value: Any) -> str:
    try:
        return f"{int(value):,}"
    except ValueError, TypeError, OverflowError:
        return "—"


def fmt_days(value: Any) -> str:
    try:
        return f"{float(value):,.0f} days"
    except ValueError, TypeError, OverflowError:
        return "—"


def metric_card(label: str, value: str, foot: str = "") -> None:
    st.markdown(
        f'<div class="metric-card"><div class="metric-label">{label}</div>'
        f'<div class="metric-value">{value}</div><div class="metric-foot">{foot}</div></div>',
        unsafe_allow_html=True,
    )


def hero(kicker: str, title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="hero"><div class="eyebrow">{kicker}</div>'
        f"<h1>{title}</h1><p>{subtitle}</p></div>",
        unsafe_allow_html=True,
    )


def chart_layout(fig: go.Figure, height: int = 360) -> go.Figure:
    """Apply the active light/dark palette to every Plotly chart."""
    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=height,
        margin=dict(l=18, r=18, t=45, b=20),
        paper_bgcolor=PANEL_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family="Inter, Arial, sans-serif", color=TEXT_COLOR, size=12),
        title_font=dict(size=15, color=TEXT_COLOR),
        legend_title_text="",
        bargap=0.22,
        bargroupgap=0.08,
        hoverlabel=dict(bgcolor=HOVER_BG, font_color=HOVER_TEXT, font_size=12),
    )
    fig.update_xaxes(
        showgrid=False, linecolor=BORDER_COLOR, zeroline=False, color=TEXT_COLOR
    )
    fig.update_yaxes(gridcolor=GRID_COLOR, zeroline=False, color=TEXT_COLOR)
    return fig


def selected_or_all(
    df: pd.DataFrame, column: str, selection: list[str]
) -> pd.DataFrame:
    if not selection or "All" in selection or column not in df.columns:
        return df
    return df[df[column].astype(str).isin(selection)]


def report_sidebar(backend: str, local_duckdb: bool = False) -> tuple[str, Any, Any]:
    with st.sidebar:
        st.markdown(
            '<div class="sidebar-brand">'
            '<svg viewBox="0 0 24 24" width="25" height="25" aria-hidden="true" '
            'focusable="false"><path d="M3.5 21V5.5L13 2v19M13 9h7.5v12" '
            'fill="none" stroke="#21B6E8" stroke-width="1.8" stroke-linecap="round" '
            'stroke-linejoin="round"/><path d="M6.5 8h2M6.5 11.5h2M6.5 15h2M16 12h2M16 15.5h2" '
            'fill="none" stroke="#21B6E8" stroke-width="1.5" stroke-linecap="round"/>'
            "</svg><span>Toronto Permits</span></div>",
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="sidebar-tagline">Follow the permit timeline story</div>',
            unsafe_allow_html=True,
        )
        page = st.radio("Explore", NAV, label_visibility="collapsed")
        st.divider()
        st.markdown("**Where the data comes from**")
        if page == "Metrics & semantic layer":
            st.caption(
                "Displays published MetricFlow results computed against the latest "
                "DuckDB warehouse; the dashboard reads the versioned result package."
            )
        elif backend == "snowflake":
            st.caption(
                "Reads the dashboard-facing dbt relations in TORONTO.ANALYTICS "
                "using the configured Snowflake role."
            )
        elif local_duckdb:
            st.caption(
                "Reads the configured local DuckDB database using read-only access."
            )
        else:
            st.markdown(
                f"[Latest published DuckDB database on GitHub ↗]({RELEASE_PAGE_URL})"
            )
            st.caption(
                "The dashboard uses Toronto Open Data processed by this project's daily pipeline."
            )
        last_successful_refresh_slot = st.empty()
        if page != "Metrics & semantic layer" and st.button(
            "Refresh data now",
            key="refresh_data_button",
            width="stretch",
            help=(
                "Re-query the Snowflake dashboard marts."
                if backend == "snowflake"
                else (
                    "Re-read the configured local DuckDB database."
                    if local_duckdb
                    else "Fetch the latest published database from GitHub. "
                    "The file is about 120 MB."
                )
            ),
        ):
            load_dashboard_data.clear()
            st.session_state["refresh_published_data"] = True
            st.rerun()
        loading_slot = st.empty()
        if page != "Metrics & semantic layer" and backend == "snowflake":
            st.caption(
                "Data is cached for up to 24 hours. Refreshing re-queries Snowflake."
            )
        elif page != "Metrics & semantic layer" and local_duckdb:
            st.caption(
                "Data is cached for up to 24 hours. Refreshing re-reads the configured "
                "local DuckDB database."
            )
        elif page != "Metrics & semantic layer":
            st.caption(
                "Data is cached for up to 24 hours. Refreshing downloads the latest "
                "published database from GitHub (about 120 MB)."
            )
        st.divider()
        st.caption(
            "These charts show patterns in the data. They do not explain what caused a permit delay."
        )
        st.markdown(
            "[Project repository ↗](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics)"
        )
    return page, loading_slot, last_successful_refresh_slot


def overview(
    active: pd.DataFrame,
    active_backlog: pd.DataFrame,
    active_backlog_by_year_type: pd.DataFrame,
    matched_lifecycle: pd.DataFrame,
    pooled_lifecycle: pd.DataFrame,
    backend: str,
) -> None:
    hero(
        "TORONTO OPEN DATA · BUILDING PERMITS",
        "Toronto's Building Permit Backlog",
        "Where the current Active backlog is concentrated, how old it is, and how long selected Closed-permit lifecycles take.",
    )

    required_columns = {
        "summary_level",
        "application_year",
        "permit_type",
        "current_active_revision_count",
        "age_eligible_count",
        "age_ineligible_count",
        "over_365_days_count",
        "over_730_days_count",
        "over_1095_days_count",
        "median_age_days",
        "p90_age_days",
        "eligible_backlog_share_pct",
    }
    missing_columns = required_columns - set(active_backlog.columns)
    if missing_columns:
        raise RuntimeError(
            "The Active backlog summary mart is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )
    overall_rows = active_backlog.loc[active_backlog["summary_level"] == "overall"]
    if len(overall_rows) != 1:
        raise RuntimeError(
            "The Active backlog summary mart must contain exactly one overall row."
        )
    overall = overall_rows.iloc[0]
    cohort = active_backlog.loc[
        active_backlog["summary_level"] == "application_year"
    ].copy()
    by_type = active_backlog.loc[
        active_backlog["summary_level"] == "permit_type"
    ].copy()
    if cohort.empty or by_type.empty:
        raise RuntimeError(
            "The Active backlog summary mart is missing year or permit-type groups."
        )

    cohort["application_year"] = number(cohort["application_year"])
    cohort["application_year_label"] = cohort["application_year"].map(
        lambda value: str(int(value)) if pd.notna(value) else "Unknown year"
    )
    cohort = cohort.sort_values("application_year", na_position="last")
    for column in (
        "current_active_revision_count",
        "age_eligible_count",
        "age_ineligible_count",
        "over_365_days_count",
        "over_730_days_count",
        "over_1095_days_count",
    ):
        cohort[column] = number(cohort[column]).fillna(0)

    total_active = int(overall["current_active_revision_count"])
    eligible_count = int(overall["age_eligible_count"])
    ineligible_count = int(overall["age_ineligible_count"])
    over_365 = int(overall["over_365_days_count"])
    over_730 = int(overall["over_730_days_count"])
    over_1095 = int(overall["over_1095_days_count"])
    median_active_age = overall["median_age_days"]
    p90_active_age = overall["p90_age_days"]
    recent_share = float(overall.get("recent_cohort_2024_plus_share_pct") or 0) / 100.0
    top_five_share = float(overall.get("top_five_over_365_share_pct") or 0) / 100.0
    latest_year_value = overall.get("latest_application_year")
    latest_year = int(latest_year_value) if pd.notna(latest_year_value) else None

    asof_values = (
        pd.to_datetime(active["as_of_date"], errors="coerce").dropna()
        if "as_of_date" in active
        else pd.Series(dtype="datetime64[ns]")
    )
    asof = (
        asof_values.iloc[0].strftime("%d %b %Y")
        if not asof_values.empty
        else "Not available"
    )
    backend_label = (
        "Snowflake warehouse"
        if backend == "snowflake"
        else "Local DuckDB database"
        if os.environ.get("DASHBOARD_DUCKDB_PATH", "").strip()
        else "Published DuckDB snapshot"
    )

    with st.container(key="story_data_context"):
        meta = st.columns([1, 1.25, 1.25], gap="medium")
        with meta[0]:
            st.markdown("**Data source**")
            st.caption("City of Toronto Open Data")
        with meta[1]:
            st.markdown("**Dashboard backend**")
            st.caption(backend_label)
        with meta[2]:
            st.markdown("**Active age measured through**")
            st.caption(asof)

    st.write("")
    cols = st.columns(4, gap="medium")
    with cols[0]:
        metric_card(
            "Current Active records", fmt_int(total_active), "Currently marked Active"
        )
    with cols[1]:
        metric_card(
            "Age-eligible records",
            fmt_int(eligible_count),
            f"{eligible_count / total_active:.1%} of Active records"
            if total_active
            else "No records",
        )
    with cols[2]:
        metric_card(
            "Open more than one year",
            fmt_int(over_365),
            f"{over_365 / eligible_count:.1%} of age-eligible records"
            if eligible_count
            else "No eligible records",
        )
    with cols[3]:
        metric_card(
            "Median Active age",
            fmt_days(median_active_age),
            f"P90: {fmt_days(p90_active_age)} · elapsed time so far",
        )

    st.markdown(
        f"**What stands out:** the five largest permit types account for {top_five_share:.1%} "
        f"of the eligible backlog older than one year; {recent_share:.1%} of the eligible "
        "backlog comes from 2024+ application cohorts."
    )

    st.divider()
    st.subheader("Where is today's Active backlog concentrated?")
    st.markdown(
        '<div class="section-note">Current Active records are grouped by the year of application. '
        "Age-ineligible records remain in the total count but are excluded from age statistics.</div>",
        unsafe_allow_html=True,
    )
    cohort_fig = go.Figure()
    cohort_fig.add_trace(
        go.Bar(
            name="Age-eligible",
            x=cohort["application_year_label"],
            y=cohort["age_eligible_count"],
            marker_color=TEAL,
        )
    )
    cohort_fig.add_trace(
        go.Bar(
            name="Age-ineligible",
            x=cohort["application_year_label"],
            y=cohort["age_ineligible_count"],
            marker_color=MUTED,
        )
    )
    cohort_fig.update_layout(
        barmode="stack", title="Current Active records by application-year cohort"
    )
    cohort_fig.update_xaxes(title="Application year")
    cohort_fig.update_yaxes(title="Current Active records")
    st.plotly_chart(chart_layout(cohort_fig, 360), width="stretch")

    cohort_table = cohort[
        [
            "application_year_label",
            "current_active_revision_count",
            "age_eligible_count",
            "age_ineligible_count",
            "eligible_backlog_share_pct",
            "over_365_days_count",
            "over_730_days_count",
            "over_1095_days_count",
            "median_age_days",
            "p90_age_days",
        ]
    ].rename(
        columns={
            "application_year_label": "Application year",
            "current_active_revision_count": "Current Active",
            "age_eligible_count": "Age eligible",
            "age_ineligible_count": "Age ineligible",
            "eligible_backlog_share_pct": "Share of eligible backlog (%)",
            "over_365_days_count": ">365 days",
            "over_730_days_count": ">730 days",
            "over_1095_days_count": ">1,095 days",
            "median_age_days": "Median age (days)",
            "p90_age_days": "P90 age (days)",
        }
    )
    with st.expander("View application-year details"):
        st.dataframe(cohort_table, hide_index=True, width="stretch")
    if latest_year:
        st.caption(f"The {latest_year} application-year cohort is partial.")
    st.caption(
        f"Age-ineligible Active records: {ineligible_count:,}. Threshold counts use age-eligible records only: "
        f"{over_365:,} over 365 days, {over_730:,} over 730 days, and {over_1095:,} over 1,095 days."
    )

    st.divider()
    st.subheader("Which permit types account for the older backlog?")
    st.markdown(
        '<div class="section-note">Ranked by the number of age-eligible Active records open for more than 365 days. '
        "Median and P90 ages are calculated from individual Active records, not averages of permit-type summaries.</div>",
        unsafe_allow_html=True,
    )
    by_type = by_type.sort_values(
        ["over_365_days_count", "current_active_revision_count"],
        ascending=[False, False],
    )
    top_types = by_type.head(10).sort_values("over_365_days_count", ascending=True)
    type_fig = px.bar(
        top_types,
        x="over_365_days_count",
        y="permit_type",
        orientation="h",
        color_discrete_sequence=[TEAL],
        title="Top permit types by Active records open over one year",
    )
    type_fig.update_xaxes(title="Age-eligible Active records open >365 days")
    type_fig.update_yaxes(title="")
    st.plotly_chart(chart_layout(type_fig, 420), width="stretch")

    type_table = by_type.head(12)[
        [
            "permit_type",
            "current_active_revision_count",
            "age_eligible_count",
            "over_365_days_count",
            "over_730_days_count",
            "over_1095_days_count",
            "median_age_days",
            "p90_age_days",
        ]
    ].rename(
        columns={
            "permit_type": "Permit type",
            "current_active_revision_count": "Current Active",
            "age_eligible_count": "Age eligible",
            "over_365_days_count": ">365 days",
            "over_730_days_count": ">730 days",
            "over_1095_days_count": ">1,095 days",
            "median_age_days": "Median age (days)",
            "p90_age_days": "P90 age (days)",
        }
    )
    with st.expander("View permit-type details"):
        st.dataframe(type_table, hide_index=True, width="stretch")

    with st.expander("Explore older backlog by application year and permit type"):
        year_type = active_backlog_by_year_type.copy()
        required_year_type = {
            "application_year",
            "permit_type",
            "over_365_days_count",
            "over_730_days_count",
            "over_1095_days_count",
            "median_age_days",
            "p90_age_days",
        }
        missing_year_type = required_year_type - set(year_type.columns)
        if missing_year_type:
            raise RuntimeError(
                "The Active year/type mart is missing columns: "
                + ", ".join(sorted(missing_year_type))
            )
        year_type["application_year"] = number(year_type["application_year"])
        year_type = year_type.dropna(subset=["application_year", "permit_type"])
        measure_options = {
            "Open more than 1 year": ("over_365_days_count", "Active revisions"),
            "Open more than 2 years": ("over_730_days_count", "Active revisions"),
            "Open more than 3 years": ("over_1095_days_count", "Active revisions"),
            "Median age": ("median_age_days", "Calendar days"),
            "90th-percentile age": ("p90_age_days", "Calendar days"),
        }
        selected_measure = st.selectbox(
            "Measure",
            list(measure_options),
            key="story_active_year_type_measure",
        )
        measure_column, measure_unit = measure_options[selected_measure]
        ranked_types = (
            year_type.groupby("permit_type")["over_365_days_count"]
            .sum()
            .sort_values(ascending=False)
            .head(12)
        )
        heatmap_rows = year_type[year_type["permit_type"].isin(ranked_types.index)]
        pivot = heatmap_rows.pivot_table(
            index="permit_type",
            columns="application_year",
            values=measure_column,
            aggfunc="first",
        )
        pivot = pivot.reindex(index=ranked_types.index).sort_index(axis=1)
        if pivot.empty:
            st.info("No application-year and permit-type groups are available.")
        else:
            heatmap = go.Figure(
                go.Heatmap(
                    z=pivot.to_numpy(dtype=float),
                    x=[str(int(year)) for year in pivot.columns],
                    y=pivot.index.astype(str).tolist(),
                    colorscale=[[0, SCALE_LOW], [1, TEAL]],
                    colorbar={"title": measure_unit},
                    hovertemplate=(
                        "Permit type: %{y}<br>Application year: %{x}<br>"
                        + f"{selected_measure}: %{{z:,.1f}}<extra></extra>"
                    ),
                )
            )
            heatmap.update_layout(
                title=f"{selected_measure} by application-year cohort and permit type",
                xaxis_title="Application year",
                yaxis_title="Permit type",
            )
            st.plotly_chart(
                chart_layout(heatmap, max(360, 30 * len(pivot.index) + 120)),
                width="stretch",
            )
            st.caption(
                "Shows the 12 permit types with the largest over-one-year backlog. "
                "Revisions without a usable application year remain in overall totals "
                "but cannot be placed in this cohort view."
            )

    st.divider()
    st.subheader("How long do Closed permits spend at each lifecycle stage?")
    st.markdown(
        '<div class="section-note">These three intervals use the same matched cohort of records marked Closed '
        "with application, issue, and completion dates all present and chronologically ordered.</div>",
        unsafe_allow_html=True,
    )
    measure_order = [
        "Application to issue",
        "Issue to completion",
        "Application to completion",
    ]
    pooled = pooled_lifecycle.copy()
    for column in ("matched_cohort_count", "median_days", "p90_days"):
        pooled[column] = number(pooled[column])
    pooled = pooled.set_index("measure").reindex(measure_order).reset_index()
    if pooled[["matched_cohort_count", "median_days", "p90_days"]].isna().any().any():
        raise RuntimeError(
            "The pooled matched lifecycle summary is missing one or more intervals."
        )
    matched_count = int(pooled["matched_cohort_count"].min())
    lifecycle_cols = st.columns(3, gap="medium")
    for column, row in zip(lifecycle_cols, pooled.to_dict("records"), strict=True):
        with column:
            metric_card(
                row["measure"],
                fmt_days(row["median_days"]),
                f"P90: {fmt_days(row['p90_days'])} · n={int(row['matched_cohort_count']):,}",
            )

    st.markdown('<div style="height: 1.25rem;"></div>', unsafe_allow_html=True)
    lifecycle_fig = go.Figure()
    lifecycle_fig.add_trace(
        go.Bar(
            name="Median",
            x=pooled["measure"],
            y=pooled["median_days"],
            marker_color=TEAL,
        )
    )
    lifecycle_fig.add_trace(
        go.Bar(
            name="90th percentile",
            x=pooled["measure"],
            y=pooled["p90_days"],
            marker_color=GOLD,
        )
    )
    lifecycle_fig.update_layout(
        barmode="group", title="Elapsed calendar days in the matched Closed cohort"
    )
    lifecycle_fig.update_xaxes(title="Lifecycle interval")
    lifecycle_fig.update_yaxes(title="Calendar days")
    st.plotly_chart(chart_layout(lifecycle_fig, 360), width="stretch")
    st.caption(
        f"Matched cohort: {matched_count:,} Closed permit records. Each interval uses the same records; "
        "these are pooled permit-level medians and P90s, not medians of year/type group summaries."
    )

    with st.expander(
        "Explore matched lifecycle cohorts by permit type and application year"
    ):
        matched_lifecycle["application_year"] = number(
            matched_lifecycle["application_year"]
        )
        matched_lifecycle["matched_cohort_count"] = number(
            matched_lifecycle["matched_cohort_count"]
        ).fillna(0)
        matched_lifecycle["median_days"] = number(matched_lifecycle["median_days"])
        matched_lifecycle["p90_days"] = number(matched_lifecycle["p90_days"])
        permit_types = sorted(
            matched_lifecycle["permit_type"].dropna().astype(str).unique()
        )
        if permit_types:
            choose_type, choose_measure = st.columns(2, gap="medium")
            selected_type = choose_type.selectbox(
                "Permit type", permit_types, key="story_matched_permit_type"
            )
            selected_measure = choose_measure.selectbox(
                "Lifecycle interval",
                measure_order,
                key="story_matched_lifecycle_measure",
            )
            selected_cohorts = matched_lifecycle.loc[
                (matched_lifecycle["permit_type"].astype(str) == selected_type)
                & (matched_lifecycle["measure"] == selected_measure)
            ].sort_values("application_year")
            selected_cohorts = selected_cohorts[
                ["application_year", "matched_cohort_count", "median_days", "p90_days"]
            ].rename(
                columns={
                    "application_year": "Application year",
                    "matched_cohort_count": "Matched records",
                    "median_days": "Median days",
                    "p90_days": "P90 days",
                }
            )
            st.dataframe(selected_cohorts, hide_index=True, width="stretch")
            st.caption(
                "Each row is one application-year × permit-type matched cohort. "
                "These group-level values should not be averaged to produce an overall permit-level median."
            )
        else:
            st.info(
                "No matched Closed lifecycle cohorts are available for permit-type exploration."
            )

    st.markdown(
        '<div class="callout"><b>Scope and interpretation:</b> Active ages are elapsed time so far for records still marked Active; '
        "they are ongoing timelines, not forecasts of completion. The Closed cohort includes only records with all three dates "
        "available and in chronological order. Recent Closed cohorts are selected from permits already marked Closed, so they "
        "must not be interpreted as proof that processing times are improving. The source covers building-permit records only; "
        "it does not measure housing units, affordability, causes of delay, or the wider development-application pipeline.</div>",
        unsafe_allow_html=True,
    )


def active_page(active: pd.DataFrame) -> None:
    hero(
        "PERMITS STILL MARKED ACTIVE",
        "Active permits: how long they have been open",
        "Explore how long permit records have been open so far, by permit type.",
    )
    asof = (
        str(active["as_of_date"].dropna().iloc[0])
        if "as_of_date" in active and active["as_of_date"].notna().any()
        else "Not available"
    )
    st.caption(
        f"Data measured as of **{asof}**. These permits are still marked Active, so the times shown are ongoing—not final completion times."
    )
    active["is_age_eligible"] = normalize_bool(active["is_age_eligible"])
    eligible = active[active["is_age_eligible"]].copy()
    eligible["age_since_application_days"] = number(
        eligible["age_since_application_days"]
    )
    eligible["application_date"] = pd.to_datetime(
        eligible["application_date"], errors="coerce"
    )
    types = sorted(eligible["permit_type"].dropna().astype(str).unique().tolist())
    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        selected_types = st.multiselect(
            "Permit type",
            types,
            default=[],
            help="Leave empty to include all permit types; select one or more to narrow the view.",
        )
    with c2:
        age_values = number(eligible["age_since_application_days"]).dropna()
        observed_max_age = max(0, int(age_values.max())) if not age_values.empty else 0
        # Keep the default slider at the full observed age range. A restriction
        # is applied only when the viewer moves the slider below this maximum.
        slider_max = max(3690, ((observed_max_age + 89) // 90) * 90)
        age_cap = st.slider(
            "Maximum age to show (days)",
            min_value=90,
            max_value=slider_max,
            value=slider_max,
            step=90,
            help="Defaults to all eligible ages. Move the slider left to apply an age cap.",
        )
    with c3:
        st.caption(
            f"Records included: **{len(eligible):,} of {len(active):,}**. Some records are left out when key dates or status information is missing or conflicting."
        )
    view = selected_or_all(eligible, "permit_type", selected_types)
    if age_cap < slider_max:
        view = view[
            view["age_since_application_days"].between(0, age_cap, inclusive="both")
        ]
    a, b, c = st.columns(3)
    with a:
        metric_card(
            "Records in this view",
            fmt_int(len(view)),
            f"Age between 0 and {age_cap:,} days"
            if age_cap < slider_max
            else "All eligible ages",
        )
    with b:
        metric_card(
            "Middle age",
            fmt_days(view["age_since_application_days"].median()),
            "Half are younger; half are older",
        )
    with c:
        metric_card(
            "90th-percentile age",
            fmt_days(view["age_since_application_days"].quantile(0.9)),
            "9 out of 10 are at or below this age",
        )
    st.markdown('<div style="height: 1.25rem;"></div>', unsafe_allow_html=True)
    left, right = st.columns([1.15, 1], gap="large")
    with left:
        hist = px.histogram(
            view,
            x="age_since_application_days",
            nbins=30,
            color_discrete_sequence=[TEAL],
            title="How long active permit records have been open",
        )
        hist.update_xaxes(title="Days since application")
        hist.update_yaxes(title="Permit records")
        st.plotly_chart(chart_layout(hist, 390), width="stretch")
    with right:
        grouped = (
            view.groupby("permit_type", as_index=False)
            .agg(
                revisions=("permit_num", "size"),
                median_age_days=("age_since_application_days", "median"),
                p90_age_days=("age_since_application_days", lambda x: x.quantile(0.9)),
            )
            .sort_values("median_age_days", ascending=False)
        )
        grouped = grouped.head(18)
        fig = px.bar(
            grouped.sort_values("median_age_days"),
            x="median_age_days",
            y="permit_type",
            orientation="h",
            color="median_age_days",
            color_continuous_scale=[SCALE_LOW, TEAL],
            hover_data={
                "revisions": ":,",
                "median_age_days": ":.0f",
                "p90_age_days": ":.0f",
            },
            labels={
                "revisions": "Records",
                "median_age_days": "Middle age (days)",
                "p90_age_days": "90th-percentile age (days)",
                "permit_type": "Permit type",
            },
            title="Middle age by permit type",
        )
        fig.update_xaxes(title="Middle age (days)")
        fig.update_yaxes(title="")
        st.plotly_chart(chart_layout(fig, 390), width="stretch")
    with st.expander("Inspect individual Active permit records"):
        show_cols = [
            c
            for c in [
                "permit_num",
                "revision_num",
                "permit_type",
                "status_normalized",
                "application_date",
                "issued_date",
                "age_since_application_days",
            ]
            if c in view.columns
        ]
        display_records = (
            view[show_cols]
            .sort_values("age_since_application_days", ascending=False)
            .rename(
                columns={
                    "permit_num": "Permit number",
                    "revision_num": "Revision",
                    "permit_type": "Permit type",
                    "status_normalized": "Status",
                    "application_date": "Application date",
                    "issued_date": "Issue date",
                    "age_since_application_days": "Days open",
                }
            )
        )
        st.dataframe(display_records, width="stretch", hide_index=True)
        st.caption(
            "These records are still marked Active in the published data. Their current age should not be read as the time it will take to finish."
        )


def lifecycle_page(summary: pd.DataFrame) -> None:
    hero(
        "PAST PERMITS MARKED CLOSED",
        "Time between key permit dates",
        "Compare how much time passed between application, issue and completion dates for permits marked Closed.",
    )
    summary = summary.copy()
    for c in [
        "application_year",
        "eligible_date_pairs",
        "negative_interval_count",
        "publishable_duration_count",
        "median_days",
        "p90_days",
        "max_days",
        "negative_interval_rate_pct",
    ]:
        if c in summary:
            summary[c] = number(summary[c])
    summary = summary[summary["measure"].notna()]
    years = sorted(summary["application_year"].dropna().astype(int).unique().tolist())
    types = sorted(summary["permit_type"].dropna().astype(str).unique().tolist())
    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        chosen_measure = st.selectbox(
            "Date interval to analyse",
            sorted(summary["measure"].unique()),
            index=(
                sorted(summary["measure"].unique()).index("Application to completion")
                if "Application to completion" in summary["measure"].unique()
                else 0
            ),
        )
    with c2:
        chosen_types = st.multiselect(
            "Permit type",
            types,
            default=[],
            help="Leave empty to include all permit types; select one or more to narrow the view.",
        )
    with c3:
        if years:
            year_range = st.slider(
                "Application year",
                min_value=min(years),
                max_value=max(years),
                value=(min(years), max(years)),
            )
        else:
            year_range = (2020, 2026)
    view = summary[summary["measure"].eq(chosen_measure)]
    if chosen_types:
        view = view[view["permit_type"].astype(str).isin(chosen_types)]
    view = view[view["application_year"].between(year_range[0], year_range[1])]
    a, b, c, d = st.columns(4)
    with a:
        metric_card(
            "Records with both dates",
            fmt_int(view["eligible_date_pairs"].sum()),
            "For the selected dates and filters",
        )
    with b:
        metric_card(
            "Usable time values",
            fmt_int(view["publishable_duration_count"].sum()),
            "Records with non-negative durations",
        )
    with c:
        metric_card(
            "Median of group medians",
            fmt_days(view["median_days"].median()),
            "Summarized by year and type",
        )
    with d:
        metric_card(
            "Median of group 90th percentiles",
            fmt_days(view["p90_days"].median()),
            "Summarized by year and type",
        )
    st.markdown('<div style="height: 1.25rem;"></div>', unsafe_allow_html=True)
    left, right = st.columns([1.2, 1], gap="large")
    with left:
        trend = view.groupby("application_year", as_index=False).agg(
            median_days=("median_days", "median"), p90_days=("p90_days", "median")
        )
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=trend["application_year"],
                y=trend["median_days"],
                mode="lines+markers",
                name="Median of year/type group medians",
                line=dict(color=TEAL, width=3),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=trend["application_year"],
                y=trend["p90_days"],
                mode="lines+markers",
                name="Median of year/type group P90s",
                line=dict(color=RED, width=3),
            )
        )
        fig.update_layout(title="Closed-permit durations by application-year cohort")
        fig.update_xaxes(title="Application year", dtick=1)
        fig.update_yaxes(title="Days")
        st.plotly_chart(chart_layout(fig, 390), width="stretch")
    with right:
        bar = px.bar(
            view.sort_values("median_days", ascending=False).head(18),
            x="median_days",
            y="permit_type",
            orientation="h",
            color="median_days",
            color_continuous_scale=[SCALE_LOW, TEAL],
            hover_data={"eligible_date_pairs": ":,", "p90_days": ":.0f"},
            labels={
                "median_days": "Typical time (days)",
                "permit_type": "Permit type",
                "eligible_date_pairs": "Records with both dates",
                "p90_days": "90th percentile (days)",
            },
            title="Typical time by permit type",
        )
        bar.update_xaxes(title="Typical time (days)")
        bar.update_yaxes(title="")
        st.plotly_chart(chart_layout(bar, 390), width="stretch")
    with st.expander("View detailed results by year and permit type"):
        table = view.sort_values(["application_year", "permit_type"]).rename(
            columns={
                "application_year": "Application year",
                "permit_type": "Permit type",
                "measure": "Date range",
                "eligible_date_pairs": "Records with both dates",
                "negative_interval_count": "Records with dates out of order",
                "publishable_duration_count": "Usable time values",
                "negative_interval_rate_pct": "Out-of-order dates (%)",
                "median_days": "Typical time (days)",
                "p90_days": "90th percentile (days)",
                "max_days": "Longest time (days)",
            }
        )
        st.dataframe(table, width="stretch", hide_index=True)
        st.caption(
            "The charts summarize groups of permits by year and type. They do not calculate one median or 90th percentile across all permits combined."
        )


def quality_page(
    active: pd.DataFrame,
    cleared_revision_count: int,
    quality_counts: dict[str, int],
    summary: pd.DataFrame,
) -> None:
    hero(
        "CHECK THE DATA",
        "Data quality and definitions",
        "Findings, analysis exclusions and date completeness are reported separately so their counts are not confused.",
    )
    # Counts and exclusion classifications are prepared and tested in dbt.
    active_n = int(quality_counts.get("active_revisions_analysed", 0))
    eligible_n = int(quality_counts.get("active_revisions_age_eligible", 0))
    excluded_n = int(quality_counts.get("active_revisions_age_ineligible", 0))
    active_findings_n = int(quality_counts.get("active_revisions_with_findings", 0))
    cleared_findings_n = int(quality_counts.get("cleared_revisions_with_findings", 0))

    active_cols = st.columns(4)
    with active_cols[0]:
        metric_card(
            "Active revisions analysed", fmt_int(active_n), "Current logical revisions"
        )
    with active_cols[1]:
        metric_card(
            "Active revisions with findings",
            fmt_int(active_findings_n),
            "Each revision counted once",
        )
    with active_cols[2]:
        metric_card(
            "Included in Active age view",
            fmt_int(eligible_n),
            f"{eligible_n / active_n:.1%} of Active revisions"
            if active_n
            else "No records",
        )
    with active_cols[3]:
        metric_card(
            "Excluded from Active age view",
            fmt_int(excluded_n),
            "Includes intentional status exclusions",
        )

    st.subheader("Active data-quality checks")
    st.caption(
        f"{active_findings_n:,} distinct Active revisions have at least one flagged issue. "
        "This count includes source conflicts and missing status or application date. "
        "Missing builder names, issue dates and geographic IDs are shown as completeness "
        "checks and do not increase this KPI. Counts by check can overlap."
    )
    active_checks = [
        {
            "Check": "Builder names disagree",
            "Records": quality_counts.get("active_builder_name_conflict", 0),
            "What it means": "Current source rows for the same revision contain different builder names, or a mix of a name and a missing value.",
            "Effect on analysis": "The builder name is withheld. Age eligibility is unchanged.",
        },
        {
            "Check": "Status values disagree",
            "Records": quality_counts.get("active_has_status_conflict", 0),
            "What it means": "Current source rows for the same revision contain different statuses, or a mix of a status and a missing value.",
            "Effect on analysis": "The status is withheld and the revision is excluded from the Active age view.",
        },
        {
            "Check": "Application dates disagree",
            "Records": quality_counts.get("active_application_date_conflict", 0),
            "What it means": "Current source rows for the same revision contain different application dates, or a mix of a date and a missing value.",
            "Effect on analysis": "The application date is withheld; age cannot be calculated for a revision otherwise eligible for the Active age view.",
        },
        {
            "Check": "Issue dates disagree",
            "Records": quality_counts.get("active_issued_date_conflict", 0),
            "What it means": "Current source rows for the same revision contain different issue dates, or a mix of a date and a missing value.",
            "Effect on analysis": "The issue date is withheld. It does not affect the Active age calculation, which uses application date and the as-of date.",
        },
        {
            "Check": "Geographic IDs disagree",
            "Records": quality_counts.get("active_geo_id_conflict", 0),
            "What it means": "Current source rows for the same revision contain different geographic IDs, or a mix of an ID and a missing value.",
            "Effect on analysis": "The geographic ID is withheld. Age eligibility is unchanged.",
        },
        {
            "Check": "Builder name missing",
            "Records": quality_counts.get("active_builder_name_missing", 0),
            "What it means": "No builder name is available and the current source rows do not disagree on the value.",
            "Effect on analysis": "Builder-level analysis is less complete; Active age eligibility is unchanged.",
        },
        {
            "Check": "Issue date missing",
            "Records": quality_counts.get("active_issued_date_missing", 0),
            "What it means": "No issue date is available and the current source rows do not disagree on the value.",
            "Effect on analysis": "This can be expected for a permit not yet issued; Active age uses application date, not issue date.",
        },
        {
            "Check": "Geographic ID missing",
            "Records": quality_counts.get("active_geo_id_missing", 0),
            "What it means": "No geographic ID is available and the current source rows do not disagree on the value.",
            "Effect on analysis": "Geographic analysis may be incomplete; Active age eligibility is unchanged.",
        },
        {
            "Check": "Status missing",
            "Records": quality_counts.get("active_status_missing", 0),
            "What it means": "No status is available and the current source rows do not disagree on status.",
            "Effect on analysis": "The revision is excluded because its status cannot be classified.",
        },
        {
            "Check": "Application date missing",
            "Records": quality_counts.get("active_application_date_missing", 0),
            "What it means": "No application date is available and the current source rows do not disagree on the date.",
            "Effect on analysis": "The revision cannot be aged if it otherwise qualifies for the Active cohort; a terminal status may already exclude it.",
        },
    ]
    active_findings = pd.DataFrame(active_checks)
    fig = px.bar(
        active_findings.sort_values("Records"),
        x="Records",
        y="Check",
        text="Records",
        orientation="h",
        color="Records",
        color_continuous_scale=[SCALE_LOW, TEAL],
        title="Active revisions by check",
    )
    fig.update_traces(
        texttemplate="%{text:,}", textposition="outside", cliponaxis=False
    )
    fig.update_xaxes(title="Logical revisions", rangemode="tozero")
    fig.update_yaxes(title="")
    st.plotly_chart(
        chart_layout(fig, max(310, 48 * len(active_findings) + 85)),
        width="stretch",
    )
    with st.expander("View Active check details"):
        st.dataframe(active_findings, width="stretch", hide_index=True)

    st.subheader("Why Active revisions are excluded from the age view")
    st.caption(
        "Each excluded revision is assigned its first applicable reason. "
        "This chart is an eligibility breakdown, not a count of data-quality defects."
    )
    exclusion_labels = {
        "status_conflict": "Status values conflict",
        "missing_status": "Status is missing",
        "terminal_or_exception_status": "Closed, Cancelled or Abandoned (intentional exclusion)",
        "application_date_conflict": "Application dates conflict",
        "missing_application_date": "Application date is missing",
        "unclassified": "Unclassified exclusion",
    }
    exclusion = pd.DataFrame(
        [
            {
                "Reason": label,
                "Records": quality_counts.get(f"active_exclusion_{reason}", 0),
            }
            for reason, label in exclusion_labels.items()
        ]
    )
    exclusion = exclusion.loc[exclusion["Records"] > 0].sort_values(
        "Records", ascending=False
    )
    if exclusion.empty:
        st.info("No Active revisions are excluded from the age analysis.")
    else:
        fig = px.bar(
            exclusion,
            x="Records",
            y="Reason",
            text="Records",
            orientation="h",
            color="Reason",
            color_discrete_sequence=[TEAL, GOLD, BLUE, RED],
            title="Excluded Active revisions by first applicable reason",
        )
        fig.update_traces(
            texttemplate="%{text:,}", textposition="outside", cliponaxis=False
        )
        fig.update_xaxes(title="Excluded logical revisions", rangemode="tozero")
        fig.update_yaxes(title="")
        st.plotly_chart(
            chart_layout(fig, max(260, min(420, 110 + 55 * len(exclusion)))),
            width="stretch",
        )
        with st.expander("View Active exclusion details"):
            st.dataframe(exclusion, width="stretch", hide_index=True)
    st.caption(
        "Closed, Cancelled and Abandoned statuses are excluded by design because "
        "they are not part of the open Active-age cohort. That exclusion does not, "
        "by itself, mean the source data is defective."
    )

    st.divider()
    cleared_cols = st.columns(2)
    with cleared_cols[0]:
        metric_card(
            "Cleared revisions analysed",
            fmt_int(cleared_revision_count),
            "All statuses in the current Cleared dataset",
        )
    with cleared_cols[1]:
        metric_card(
            "Cleared revisions with flagged issues",
            fmt_int(cleared_findings_n),
            "Each revision counted once; checks can overlap",
        )

    st.subheader("Cleared data-quality checks")
    st.caption(
        f"{cleared_findings_n:,} distinct Cleared revisions have at least one flagged issue. "
        "The unique count includes source conflicts, missing status and out-of-order "
        "dates across all statuses. Missing builder names and geographic IDs are "
        "completeness checks, not part of this KPI; missing lifecycle dates are shown "
        "in the date-completeness table. One revision can contribute to more than one check."
    )
    cleared_checks = [
        {
            "Check": "Builder names disagree",
            "Records": quality_counts.get("builder_name_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on builder name, including missing versus populated values.",
            "Effect on analysis": "The builder name is withheld; other fields remain available when valid.",
        },
        {
            "Check": "Status values disagree",
            "Records": quality_counts.get("has_status_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on status, including missing versus populated values.",
            "Effect on analysis": "The status is withheld; the revision cannot be identified reliably as Closed for lifecycle summaries.",
        },
        {
            "Check": "Application dates disagree",
            "Records": quality_counts.get("application_date_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on application date.",
            "Effect on analysis": "The application date is withheld; durations that require it cannot be calculated.",
        },
        {
            "Check": "Issue dates disagree",
            "Records": quality_counts.get("issued_date_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on issue date.",
            "Effect on analysis": "The issue date is withheld; durations that require it cannot be calculated.",
        },
        {
            "Check": "Completion dates disagree",
            "Records": quality_counts.get("completed_date_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on completion date.",
            "Effect on analysis": "The completion date is withheld; durations that require it cannot be calculated.",
        },
        {
            "Check": "Geographic IDs disagree",
            "Records": quality_counts.get("geo_id_conflict", 0),
            "What it means": "Current source rows for the same revision disagree on geographic ID.",
            "Effect on analysis": "The geographic ID is withheld; lifecycle durations are unaffected.",
        },
        {
            "Check": "Issue date before application date",
            "Records": quality_counts.get("application_to_issue_negative", 0),
            "What it means": "The recorded issue date occurs before the recorded application date.",
            "Effect on analysis": "For a Closed revision, only the Application-to-issue duration is excluded; other date comparisons are evaluated separately.",
        },
        {
            "Check": "Completion date before application date",
            "Records": quality_counts.get("application_to_completion_negative", 0),
            "What it means": "The recorded completion date occurs before the recorded application date.",
            "Effect on analysis": "For a Closed revision, only the Application-to-completion duration is excluded; other date comparisons are evaluated separately.",
        },
        {
            "Check": "Completion date before issue date",
            "Records": quality_counts.get("issue_to_completion_negative", 0),
            "What it means": "The recorded completion date occurs before the recorded issue date.",
            "Effect on analysis": "For a Closed revision, only the Issue-to-completion duration is excluded; other date comparisons are evaluated separately.",
        },
        {
            "Check": "Builder name missing",
            "Records": quality_counts.get("cleared_builder_name_missing", 0),
            "What it means": "No builder name was observed in the current source rows and there is no builder-name conflict.",
            "Effect on analysis": "Builder-level analysis is less complete; lifecycle durations are unaffected.",
        },
        {
            "Check": "Geographic ID missing",
            "Records": quality_counts.get("cleared_geo_id_missing", 0),
            "What it means": "No geographic ID is available and the current source rows do not disagree on the value.",
            "Effect on analysis": "Geographic analysis may be incomplete; lifecycle durations are unaffected.",
        },
        {
            "Check": "Status missing",
            "Records": quality_counts.get("cleared_status_missing", 0),
            "What it means": "No status is available and the current source rows do not disagree on status.",
            "Effect on analysis": "The revision cannot be assigned to the Closed lifecycle cohort.",
        },
    ]
    cleared_findings = pd.DataFrame(cleared_checks)
    fig = px.bar(
        cleared_findings.sort_values("Records"),
        x="Records",
        y="Check",
        text="Records",
        orientation="h",
        color="Records",
        color_continuous_scale=[SCALE_LOW, TEAL],
        title="Cleared revisions by check",
    )
    fig.update_traces(
        texttemplate="%{text:,}", textposition="outside", cliponaxis=False
    )
    fig.update_xaxes(title="Logical revisions", rangemode="tozero")
    fig.update_yaxes(title="")
    st.plotly_chart(
        chart_layout(fig, max(380, 42 * len(cleared_findings) + 100)),
        width="stretch",
    )
    with st.expander("View Cleared check details"):
        st.dataframe(cleared_findings, width="stretch", hide_index=True)

    st.subheader("Cleared date completeness")
    st.caption(
        "These are availability counts, not a list of defects. They cover every "
        "current logical Cleared revision, regardless of status. 'Missing / "
        "unavailable' excludes revisions already counted as having conflicting values."
    )
    application_populated = quality_counts.get("application_date_usable", 0)
    application_missing = quality_counts.get("application_date_no_usable", 0)
    application_conflicts = quality_counts.get("application_date_conflict", 0)
    issue_populated = quality_counts.get("issued_date_usable", 0)
    issue_missing = quality_counts.get("issued_date_no_usable", 0)
    issue_conflicts = quality_counts.get("issued_date_conflict", 0)
    completion_populated = quality_counts.get("completion_date_usable", 0)
    completion_missing = quality_counts.get("completion_date_no_usable", 0)
    completion_conflicts = quality_counts.get("completed_date_conflict", 0)
    closed_count = quality_counts.get("closed_revision_count", 0)

    coverage_table = pd.DataFrame(
        [
            {
                "Date field": "Application date",
                "Populated": application_populated,
                "Missing / unavailable": application_missing,
                "Conflicting source values": application_conflicts,
            },
            {
                "Date field": "Issue date",
                "Populated": issue_populated,
                "Missing / unavailable": issue_missing,
                "Conflicting source values": issue_conflicts,
            },
            {
                "Date field": "Completion date",
                "Populated": completion_populated,
                "Missing / unavailable": completion_missing,
                "Conflicting source values": completion_conflicts,
            },
        ]
    )
    st.dataframe(coverage_table, width="stretch", hide_index=True)
    if any(
        populated + missing + conflicts != cleared_revision_count
        for populated, missing, conflicts in [
            (application_populated, application_missing, application_conflicts),
            (issue_populated, issue_missing, issue_conflicts),
            (completion_populated, completion_missing, completion_conflicts),
        ]
    ):
        st.warning(
            "One or more date-coverage rows do not reconcile to the total Cleared "
            "revision count. Review the fact-level date and conflict flags."
        )

    matched_closed = quality_counts.get("closed_matched_three_date_count", 0)
    excluded_conflicts = quality_counts.get("closed_excluded_conflicting_date_count", 0)
    excluded_missing = quality_counts.get("closed_excluded_missing_date_count", 0)
    excluded_order = quality_counts.get("closed_excluded_out_of_order_date_count", 0)
    excluded_closed = excluded_conflicts + excluded_missing + excluded_order

    st.subheader("Which Closed records support lifecycle analysis?")
    funnel_cols = st.columns(3, gap="medium")
    with funnel_cols[0]:
        metric_card(
            "Current Cleared revisions",
            fmt_int(cleared_revision_count),
            f"{cleared_revision_count - closed_count:,} not marked Closed",
        )
    with funnel_cols[1]:
        metric_card(
            "Marked Closed", fmt_int(closed_count), "Selected by the recorded status"
        )
    with funnel_cols[2]:
        metric_card(
            "Matched lifecycle cohort",
            fmt_int(matched_closed),
            "All three dates present and ordered",
        )
    if closed_count != matched_closed + excluded_closed:
        st.warning(
            "The Closed lifecycle cohort does not reconcile. Review the dbt data-quality checks."
        )
    with st.expander("Why some Closed revisions are not in the matched cohort"):
        exclusion_table = pd.DataFrame(
            [
                {
                    "First applicable exclusion": "Conflicting lifecycle dates",
                    "Revisions": excluded_conflicts,
                },
                {
                    "First applicable exclusion": "Missing / unavailable lifecycle date",
                    "Revisions": excluded_missing,
                },
                {
                    "First applicable exclusion": "Dates out of chronological order",
                    "Revisions": excluded_order,
                },
            ]
        )
        st.dataframe(exclusion_table, hide_index=True, width="stretch")
        st.caption(
            "Exclusion reasons are mutually exclusive. Conflicts take priority, then missing dates, "
            "then out-of-order dates. Matched plus excluded revisions reconcile to all records marked Closed."
        )

    st.caption(
        "A populated completion date is not proof of successful completion. Only records marked Closed "
        "enter the lifecycle cohort; each interval is elapsed calendar days, not business days."
    )

    with st.expander("Definitions and interpretation"):
        st.subheader("What the numbers mean")
        st.markdown("""
        - **Logical revision:** one permit number, revision number and permit type. Physical source rows for the same revision are consolidated before these checks.
        - **Revisions with at least one finding:** each affected logical revision is counted once, even when it has several findings.
        - **Findings by check:** counts are separate for each check and can overlap; do not add them to calculate unique affected revisions.
        - **Conflicting values:** current source rows disagree, including a mix of missing and populated values. The disputed value is withheld instead of arbitrarily selecting one.
        - **Missing / unavailable field:** no value is available and no conflict was detected. Missing values can be expected for some attributes, permit statuses or lifecycle stages and are not automatically errors.
        - **Missing / unavailable date:** no consistent date is available and no date conflict was detected. Missing dates can be expected for some statuses and are not automatically errors.
        - **Negative interval:** the recorded dates run backwards. The corresponding duration is excluded from that measure's published statistics; other durations for the same revision can still be used. Durations are elapsed calendar days, not business days.
        - **Active age:** calendar days from application date to the dashboard's as-of date. It is an ongoing age, not a completed lifecycle duration.
        - **Lifecycle duration:** elapsed calendar days between the selected dates for a revision marked Closed. The dashboard does not infer why a permit took longer.
        - **Median:** half of the duration values are below it and half are above it.
        - **90th percentile:** 9 out of 10 duration values are at or below this value; it highlights the higher end of the distribution.
        - **Grouped summaries:** the charts compare statistics calculated within each application-year and permit-type group, rather than one statistic calculated across all permits.
        """)
        st.subheader("What to keep in mind")
        st.markdown("""
        - Toronto's published data changes over time, so counts and results can change after a new release.
        - The latest application year is incomplete; compare it carefully with earlier years.
        - DuckDB uses the latest published database from GitHub; Snowflake queries the configured analytics relations. Neither backend queries the City website directly.
        - Data is cached for up to 24 hours, unless **Refresh data now** is selected.
        - These charts describe what appears in the data. They do not show why a permit took longer.
        """)


@st.cache_data(ttl=900, max_entries=1, show_spinner=False)
def load_metricflow_results() -> dict[str, Any]:
    """Load the latest precomputed MetricFlow results from the public Release."""
    response = requests.get(
        METRICFLOW_RELEASE_URL,
        timeout=(10, 30),
        headers={"User-Agent": "toronto-housing-bottleneck-streamlit/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("backend") != "duckdb"
    ):
        raise RuntimeError(
            "The published MetricFlow results have an unsupported format."
        )
    required = {
        "active_overall",
        "active_by_permit_type",
        "active_by_year",
        "cleared_overall",
        "cleared_by_permit_type",
        "cleared_by_year",
    }
    missing = sorted(required - set(payload))
    if missing:
        raise RuntimeError(
            "The published MetricFlow results are missing sections: "
            + ", ".join(missing)
        )
    return payload


def metricflow_page() -> None:
    hero(
        "GOVERNED METRICS · OPEN-SOURCE METRICFLOW",
        "Permit metrics, defined once",
        "A reusable semantic layer defines how Active backlog and Closed-permit lifecycle metrics are calculated. MetricFlow executes those definitions against the published DuckDB warehouse; this page presents the latest versioned results.",
    )
    if st.button(
        "Refresh MetricFlow results",
        key="refresh_metricflow_results",
        help="Fetch the latest published MetricFlow result package from GitHub.",
    ):
        load_metricflow_results.clear()
        st.rerun()

    try:
        results = load_metricflow_results()
    except Exception as exc:
        st.info(
            "MetricFlow results have not been published yet, or the latest result "
            "package is temporarily unavailable. The existing dashboard data is "
            "unaffected."
        )
        st.markdown(
            "[MetricFlow workflow runs ↗](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics/actions/workflows/metricflow-metrics.yml)"
        )
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        return

    active_rows = results.get("active_overall") or []
    cleared_rows = results.get("cleared_overall") or []
    if not active_rows or not cleared_rows:
        st.warning("The latest MetricFlow result package contains no overall metrics.")
        return

    active = active_rows[0]
    cleared = cleared_rows[0]
    st.caption(
        "Precomputed from the published DuckDB warehouse · "
        f"Generated {format_refresh_timestamp(results.get('generated_at_utc'))} · "
        f"Warehouse refresh {format_refresh_timestamp(results.get('warehouse_refresh_timestamp_utc'))} · "
        f"Mode 1 run {results.get('warehouse_refresh_run_id', 'unknown')}"
    )

    st.subheader("Current Active permit backlog")
    active_cards = st.columns(4)
    active_metrics = [
        (
            "Current logical revisions",
            fmt_int(active.get("active_permit_revision_count")),
            "One row per current permit revision",
        ),
        (
            "Age-eligible revisions",
            fmt_int(active.get("eligible_active_permit_revision_count")),
            "Has usable status and application date",
        ),
        (
            "Median current age",
            fmt_days(active.get("median_active_permit_age_days")),
            "Eligible current permits only",
        ),
        (
            "Approx. 90th-percentile age",
            fmt_days(active.get("p90_active_permit_age_days")),
            "Eligible current permits only",
        ),
    ]
    for column, (label, value, foot) in zip(active_cards, active_metrics, strict=True):
        with column:
            metric_card(label, value, foot)

    active_type = pd.DataFrame(results.get("active_by_permit_type") or [])
    active_year = pd.DataFrame(results.get("active_by_year") or [])
    active_metric_options = {
        "Current revisions": ("active_permit_revision_count", "Revisions"),
        "Age-eligible revisions": (
            "eligible_active_permit_revision_count",
            "Revisions",
        ),
        "Median current age": ("median_active_permit_age_days", "Calendar days"),
        "Approx. P90 current age": ("p90_active_permit_age_days", "Calendar days"),
    }
    selected_active_label = st.selectbox(
        "Active measure",
        list(active_metric_options),
        key="metricflow_active_group_measure",
    )
    active_metric, active_unit = active_metric_options[selected_active_label]
    active_chart_cols = st.columns(2)
    with active_chart_cols[0]:
        st.markdown(f"**{selected_active_label} by permit type**")
        dimension = "active_permit_revision__permit_type"
        if not active_type.empty and {dimension, active_metric}.issubset(
            active_type.columns
        ):
            active_type[active_metric] = number(active_type[active_metric])
            active_type = active_type.sort_values(active_metric, ascending=True)
            fig = px.bar(
                active_type,
                x=active_metric,
                y=dimension,
                orientation="h",
                labels={active_metric: active_unit, dimension: "Permit type"},
            )
            fig.update_traces(marker_color=TEAL)
            st.plotly_chart(
                chart_layout(fig, max(340, 34 * len(active_type) + 100)),
                width="stretch",
            )
        else:
            st.caption("No permit-type breakdown is available for this measure.")
    with active_chart_cols[1]:
        st.markdown(f"**{selected_active_label} by application year**")
        year_dimension = "metric_time__year"
        if not active_year.empty and {year_dimension, active_metric}.issubset(
            active_year.columns
        ):
            active_year[year_dimension] = number(active_year[year_dimension])
            active_year[active_metric] = number(active_year[active_metric])
            active_year = active_year.sort_values(year_dimension)
            fig = px.line(
                active_year,
                x=year_dimension,
                y=active_metric,
                markers=True,
                labels={active_metric: active_unit, year_dimension: "Application year"},
            )
            fig.update_traces(line=dict(color=TEAL), marker=dict(color=TEAL))
            st.plotly_chart(chart_layout(fig, 360), width="stretch")
        else:
            st.caption("No application-year breakdown is available for this measure.")

    st.divider()
    st.subheader("Closed-permit lifecycle")
    cleared_metrics = [
        (
            "Median application → issue",
            fmt_days(cleared.get("median_application_to_issue_days")),
            f"Approx. P90: {fmt_days(cleared.get('p90_application_to_issue_days'))} · "
            f"n={fmt_int(cleared.get('closed_application_to_issue_valid_revision_count'))} valid Closed revisions",
        ),
        (
            "Median issue → completion",
            fmt_days(cleared.get("median_issue_to_completion_days")),
            f"Approx. P90: {fmt_days(cleared.get('p90_issue_to_completion_days'))} · "
            f"n={fmt_int(cleared.get('closed_issue_to_completion_valid_revision_count'))} valid Closed revisions",
        ),
        (
            "Median application → completion",
            fmt_days(cleared.get("median_application_to_completion_days")),
            f"Approx. P90: {fmt_days(cleared.get('p90_application_to_completion_days'))} · "
            f"n={fmt_int(cleared.get('closed_application_to_completion_valid_revision_count'))} valid Closed revisions",
        ),
    ]
    lifecycle_cols = st.columns(3, gap="medium")
    for column, (label, value, foot) in zip(
        lifecycle_cols, cleared_metrics, strict=True
    ):
        with column:
            metric_card(label, value, foot)
    st.caption(
        f"{fmt_int(cleared.get('cleared_permit_revision_count'))} current Cleared revisions; "
        "lifecycle durations are calculated only for records marked Closed."
    )

    cleared_type = pd.DataFrame(results.get("cleared_by_permit_type") or [])
    cleared_year = pd.DataFrame(results.get("cleared_by_year") or [])
    interval_metrics = {
        "Application to issue": (
            "median_application_to_issue_days",
            "p90_application_to_issue_days",
            "closed_application_to_issue_valid_revision_count",
        ),
        "Issue to completion": (
            "median_issue_to_completion_days",
            "p90_issue_to_completion_days",
            "closed_issue_to_completion_valid_revision_count",
        ),
        "Application to completion": (
            "median_application_to_completion_days",
            "p90_application_to_completion_days",
            "closed_application_to_completion_valid_revision_count",
        ),
    }
    selected_interval = st.selectbox(
        "Closed lifecycle interval",
        list(interval_metrics),
        index=2,
        key="metricflow_cleared_interval",
    )
    selected_statistic = st.selectbox(
        "Statistic",
        ["Median", "Approx. P90", "Valid records"],
        key="metricflow_cleared_statistic",
    )
    statistic_index = {
        "Median": 0,
        "Approx. P90": 1,
        "Valid records": 2,
    }[selected_statistic]
    cleared_metric = interval_metrics[selected_interval][statistic_index]
    cleared_unit = (
        "Valid Closed revisions"
        if selected_statistic == "Valid records"
        else "Calendar days"
    )
    cleared_chart_cols = st.columns(2)
    dimension = "cleared_permit_revision__permit_type"
    with cleared_chart_cols[0]:
        chart_statistic = (
            "Valid Closed records"
            if selected_statistic == "Valid records"
            else selected_statistic
        )
        st.markdown(
            f"**{chart_statistic} — {selected_interval.lower()} by permit type**"
        )
        if not cleared_type.empty and {dimension, cleared_metric}.issubset(
            cleared_type.columns
        ):
            view = cleared_type.copy()
            view[cleared_metric] = number(view[cleared_metric])
            view = view.sort_values(cleared_metric, ascending=True)
            fig = px.bar(
                view,
                x=cleared_metric,
                y=dimension,
                orientation="h",
                labels={cleared_metric: cleared_unit, dimension: "Permit type"},
            )
            fig.update_traces(marker_color=TEAL)
            st.plotly_chart(
                chart_layout(fig, max(340, 34 * len(view) + 100)),
                width="stretch",
            )
        else:
            st.caption(
                "No permit-type lifecycle breakdown is available for this measure."
            )
    with cleared_chart_cols[1]:
        st.markdown(f"**{chart_statistic} — {selected_interval.lower()} by year**")
        year_dimension = "metric_time__year"
        if not cleared_year.empty and {year_dimension, cleared_metric}.issubset(
            cleared_year.columns
        ):
            view = cleared_year.copy()
            view[year_dimension] = number(view[year_dimension])
            view[cleared_metric] = number(view[cleared_metric])
            view = view.sort_values(year_dimension)
            fig = px.line(
                view,
                x=year_dimension,
                y=cleared_metric,
                markers=True,
                labels={
                    cleared_metric: cleared_unit,
                    year_dimension: "Application year",
                },
            )
            fig.update_traces(line=dict(color=TEAL), marker=dict(color=TEAL))
            st.plotly_chart(chart_layout(fig, 360), width="stretch")
        else:
            st.caption(
                "No application-year lifecycle breakdown is available for this measure."
            )
    st.caption(
        "Valid-record counts are interval-specific: a Closed revision is counted only when that date pair "
        "has both dates and a non-negative elapsed duration. The three intervals can therefore have different denominators."
    )

    with st.expander("Semantic models and metric definitions"):
        st.markdown(
            "**Active permit revisions** — one row per current logical permit revision. "
            "Age metrics include only eligible revisions; active age is right-censored "
            "and is not a completed lifecycle duration."
        )
        st.markdown(
            "- `active_permit_revision_count`: current logical Active revisions.\n"
            "- `eligible_active_permit_revision_count`: revisions with a usable age.\n"
            "- `median_active_permit_age_days` and `p90_active_permit_age_days`: distribution of current eligible permit age."
        )
        st.markdown(
            "**Cleared permit revisions** — one row per current logical Cleared revision. "
            "Lifecycle duration metrics are restricted to records marked Closed; "
            "negative or unavailable durations are excluded by the dbt fact model."
        )
        st.markdown(
            "- `cleared_permit_revision_count`: current logical Cleared revisions.\n"
            "- Median and 90th-percentile application-to-issue, application-to-completion, and issue-to-completion days."
        )
        st.caption(
            "The definitions live with the dbt model properties in "
            "`dbt/models/marts/schema.yml`; MetricFlow compiles them into SQL."
        )


def main() -> None:
    try:
        backend = selected_dashboard_backend()
        duckdb_path = (
            os.environ.get("DASHBOARD_DUCKDB_PATH", "").strip()
            if backend == "duckdb"
            else ""
        )
    except Exception as exc:
        st.error("The dashboard backend selection could not be loaded.")
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        st.stop()

    page, loading_slot, last_successful_refresh_slot = report_sidebar(
        backend, local_duckdb=bool(duckdb_path)
    )
    force_refresh = bool(st.session_state.pop("refresh_published_data", False))
    if page == "Metrics & semantic layer":
        metricflow_page()
        st.divider()
        st.caption(
            "Toronto Housing Bottleneck Analytics · Metric definitions are maintained "
            "in dbt and evaluated by open-source MetricFlow"
        )
        return

    try:
        snowflake_config = (
            snowflake_dashboard_settings() if backend == "snowflake" else {}
        )
        config_fingerprint = (
            hashlib.sha256(
                json.dumps(snowflake_config, sort_keys=True, default=str).encode(
                    "utf-8"
                )
            ).hexdigest()
            if backend == "snowflake"
            else ""
        )
    except Exception as exc:
        st.error("The selected warehouse backend configuration could not be loaded.")
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        st.stop()
    last_loaded_at = float(st.session_state.get("_dashboard_loaded_at", 0.0))
    cache_may_have_expired = (time() - last_loaded_at) >= CACHE_TTL_SECONDS
    backend_key = (backend, config_fingerprint, duckdb_path)
    backend_changed = st.session_state.get("_dashboard_backend_key") != backend_key
    show_loading_status = (
        force_refresh
        or backend_changed
        or not st.session_state.get("_dashboard_loaded", False)
        or cache_may_have_expired
    )

    try:
        if show_loading_status:
            status_label = (
                "Refreshing dashboard data…"
                if force_refresh
                else f"Preparing {backend} dashboard data…"
            )
            # The placeholder is positioned immediately below the refresh button.
            # This puts the active loader beside the control users just clicked.
            with loading_slot.container():
                # Use a styled HTML panel for deterministic colors in all Streamlit themes.
                loading_panel = st.empty()
                if backend == "snowflake":
                    loading_detail = (
                        "Querying the Snowflake dashboard marts using read-only access."
                    )
                elif duckdb_path:
                    loading_detail = "Reading the configured local DuckDB database in read-only mode."
                elif force_refresh:
                    loading_detail = (
                        "Downloading the latest published database from GitHub. "
                        "It is about 120 MB, so this can take a minute."
                    )
                else:
                    loading_detail = (
                        "Checking the published data. The first load may download "
                        "a database of about 120 MB; later page changes usually "
                        "use cached results."
                    )
                loading_panel.markdown(
                    f"""
                    <div class="dashboard-loading-panel" role="status"
                         style="background:#303B46 !important; background-color:#303B46 !important; color:#F3F6F9 !important; padding:.8rem .85rem; border:1px solid #687782; border-radius:10px;">
                      <div style="background:transparent !important; color:#F3F6F9 !important; -webkit-text-fill-color:#F3F6F9 !important; font-weight:700;">{status_label}</div>
                      <div style="background:transparent !important; color:#F3F6F9 !important; -webkit-text-fill-color:#F3F6F9 !important; font-size:.82rem; line-height:1.4; margin-top:.35rem;">{loading_detail}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                try:
                    (
                        active,
                        summary,
                        active_backlog,
                        active_backlog_by_year_type,
                        matched_lifecycle,
                        pooled_lifecycle,
                        cleared_count,
                        quality_counts,
                        last_successful_refresh_utc,
                        last_successful_source_extraction_utc,
                    ) = load_dashboard_data(backend, config_fingerprint, duckdb_path)
                except Exception:
                    loading_panel.markdown(
                        """
                        <div role="status" style="background:#303B46 !important; background-color:#303B46 !important; color:#F3F6F9 !important; padding:.8rem .85rem; border:1px solid #687782; border-radius:10px;">
                          <div style="color:#F3F6F9 !important; -webkit-text-fill-color:#F3F6F9 !important; font-weight:700;">Could not load dashboard data</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                    raise
                else:
                    loading_panel.empty()
        else:
            # Normal filter/navigation reruns use the cache without flashing a
            # loader. A first load and every manual refresh show the status above.
            (
                active,
                summary,
                active_backlog,
                active_backlog_by_year_type,
                matched_lifecycle,
                pooled_lifecycle,
                cleared_count,
                quality_counts,
                last_successful_refresh_utc,
                last_successful_source_extraction_utc,
            ) = load_dashboard_data(backend, config_fingerprint, duckdb_path)
        st.session_state["_dashboard_loaded"] = True
        st.session_state["_dashboard_backend_key"] = backend_key
        if show_loading_status:
            st.session_state["_dashboard_loaded_at"] = time()
    except Exception as exc:
        st.error("The dashboard could not load the selected backend's permit data.")
        if backend == "duckdb" and duckdb_path:
            st.markdown(
                "Check `DASHBOARD_DUCKDB_PATH` and confirm the local database exists "
                "and contains all seven required dbt relations."
            )
        elif backend == "duckdb":
            st.markdown(
                "Check your connection and try **Refresh data now**. The app downloads "
                "the latest published database from the public "
                f"[GitHub release]({RELEASE_PAGE_URL})."
            )
        else:
            st.markdown(
                "Check the Snowflake connection settings, the dashboard role's "
                "read-only grants, and whether all seven required dbt relations "
                "exist in the configured analytics schema. The app will not silently "
                "switch to DuckDB when Snowflake fails."
            )
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        st.stop()

    source_extraction_display = format_refresh_timestamp(
        last_successful_source_extraction_utc
    )
    last_refresh_display = format_refresh_timestamp(last_successful_refresh_utc)
    with last_successful_refresh_slot.container():
        st.divider()
        st.markdown(
            f"""<div style="padding-top:.15rem;padding-bottom:.65rem;">
              <div style="font-size:.70rem;font-weight:750;letter-spacing:.10em;
                          color:#C0CAD3;margin-bottom:.35rem;">
                LATEST SOURCE EXTRACTION IN LOADED DATA
              </div>
              <div style="font-weight:750;color:#F4F7FA;font-size:1rem;
                          line-height:1.35;">{source_extraction_display}</div>
              <div style="font-size:.75rem;color:#C0CAD3;margin-top:.25rem;">
                Latest complete Active + Cleared snapshot loaded into this warehouse.
                New-format archives show extraction completion; older archives use
                their recorded extraction timestamp.
              </div>
              <div style="font-size:.70rem;font-weight:750;letter-spacing:.10em;
                          color:#C0CAD3;margin-top:.9rem;margin-bottom:.35rem;">
                LAST SUCCESSFUL WAREHOUSE REFRESH
              </div>
              <div style="font-weight:750;color:#F4F7FA;font-size:1rem;
                          line-height:1.35;">{last_refresh_display}</div>
              <div style="font-size:.75rem;color:#C0CAD3;margin-top:.25rem;">
                When this backend's dashboard tables were last successfully built
                and validated. Times are shown in Toronto local time (ET).
              </div>
            </div>""",
            unsafe_allow_html=True,
        )

    if page == "The story":
        overview(
            active.copy(),
            active_backlog.copy(),
            active_backlog_by_year_type.copy(),
            matched_lifecycle.copy(),
            pooled_lifecycle.copy(),
            backend,
        )
    elif page == "Active permits":
        active_page(active.copy())
    elif page == "Closed-permit timelines":
        lifecycle_page(summary.copy())
    else:
        quality_page(active.copy(), cleared_count, quality_counts, summary.copy())

    st.divider()
    st.caption(
        "Toronto Housing Bottleneck Analytics · Source: City of Toronto Open Data · "
        "Updated through the project's published data pipeline · Patterns shown are descriptive, not causal"
    )


if __name__ == "__main__":
    main()
