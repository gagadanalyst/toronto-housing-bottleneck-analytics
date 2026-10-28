from __future__ import annotations

import hashlib
import json
import os
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
from refresh_metadata import format_refresh_timestamp
from snowflake_backend import load_snowflake_dashboard_data
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
DBT_SCHEMA = "analytics"
CACHE_TTL_SECONDS = 24 * 60 * 60
NAV = [
    "The story",
    "Active permits",
    "Closed-permit timelines",
    "Data quality & definitions",
]


def current_theme_type() -> str:
    """Return the active Streamlit theme, with a safe fallback for older versions."""
    try:
        theme_type = getattr(st.context.theme, "type", None)
        if theme_type in {"light", "dark"}:
            return theme_type
    except AttributeError, RuntimeError:
        pass
    try:
        theme_base = st.get_option("theme.base")
        if theme_base in {"light", "dark"}:
            return theme_base
    except Exception:
        pass
    return "light"


THEME_TYPE = "light"  # Use the Tableau-inspired light palette consistently
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
  --app-page-bg: {PAGE_BG};
  --app-sidebar-bg: {SIDEBAR_BG};
  --app-panel-bg: {PANEL_BG};
  --app-text: {TEXT_COLOR};
  --app-muted: {MUTED};
  --app-border: {BORDER_COLOR};
  --app-link: {LINK_COLOR};
  --app-sidebar-text: #F4F7FA;
  --app-sidebar-muted: #C0CAD3;
  --app-sidebar-link: #28B8E8;
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
  background: linear-gradient(120deg, #FFFFFF 0%, #EAF4F8 100%);
  border: 1px solid #E1E7ED;
  padding: 1.35rem 1.8rem 1.45rem;
  border-radius: 15px;
  color: #28343E;
  margin-bottom: .8rem;
  text-align: center;
}}
.hero h1 {{
  color: #28343E !important;
  margin: 0 auto .55rem auto;
  font-size: 1.9rem;
  font-weight: 750;
  line-height: 1.22;
  letter-spacing: -.02em;
  text-align: center;
}}
.hero p {{
  color: #566573 !important;
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
  color: #168DB8;
  margin: 0 auto .58rem auto;
  text-align: center;
}}
.section-note {{ color: var(--app-muted); font-size: .94rem; line-height: 1.55; margin-top: -.25rem; margin-bottom: 1rem; }}
.metric-card {{
  border: 1px solid #DFE6EC;
  border-top: 3px solid #21B6E8;
  background: #FFFFFF;
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
.metric-label {{ color: #667582; font-size: .94rem; line-height: 1.35; font-weight: 650; margin-bottom: .52rem; text-align: center; }}
.metric-value {{ color: #26343E; font-size: 2.2rem; line-height: 1.12; font-weight: 800; letter-spacing: -.035em; text-align: center; }}
.metric-foot {{ color: #687783; font-size: .88rem; line-height: 1.45; margin-top: .55rem; text-align: center; max-width: 100%; }}
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
  background-color: #E9EEF3 !important;
  border-color: #D0DAE4 !important;
}}
div[data-testid="stMultiSelect"] [data-baseweb="tag"] {{
  background-color: #DCE6EE !important;
  color: #28343E !important;
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
  background: #E9EEF3 !important;
  background-color: #E9EEF3 !important;
  border: 1px solid #D9E1E9 !important;
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
    backend: str = "duckdb", config_fingerprint: str = ""
) -> tuple[pd.DataFrame, pd.DataFrame, int, dict[str, int], Any | None]:
    """Load dashboard data from the explicitly selected backend.

    config_fingerprint participates in Streamlit's cache key so a change to
    Snowflake connection settings cannot reuse results from the prior settings.
    """
    if backend == "snowflake":
        return load_snowflake_dashboard_data(snowflake_dashboard_settings())
    if backend != "duckdb":
        raise ValueError("Unsupported dashboard backend.")

    with TemporaryDirectory(prefix="toronto-housing-dashboard-") as temp_dir:
        # Keep the filename aligned with the dbt warehouse/catalog name.
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
                (DBT_SCHEMA, "mart_cleared_lifecycle_by_year_type"),
                (DBT_SCHEMA, "fct_cleared_permit_revision"),
            }
            missing = sorted(required - available)
            if missing:
                missing_names = ", ".join(
                    f"{schema}.{table}" for schema, table in missing
                )
                raise RuntimeError(
                    "The downloaded release does not contain required dbt relations: "
                    f"{missing_names}. The release may not yet contain a dbt-built warehouse."
                )

            active = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_active_permit_age"
            ).fetchdf()
            summary = connection.execute(
                f"SELECT * FROM {DBT_SCHEMA}.mart_cleared_lifecycle_by_year_type"
            ).fetchdf()

            # Data-quality page needs only aggregate flag counts, not the entire
            # cleared fact table in application memory.
            flag_fields = [
                "builder_name_conflict",
                "has_status_conflict",
                "application_date_conflict",
                "issued_date_conflict",
                "completed_date_conflict",
                "geo_id_conflict",
                "application_to_issue_negative",
                "application_to_completion_negative",
                "issue_to_completion_negative",
            ]
            flag_expressions = ", ".join(
                f"SUM(CASE WHEN COALESCE({field}, FALSE) THEN 1 ELSE 0 END) AS {field}"
                for field in flag_fields
            )
            result = connection.execute(
                "SELECT COUNT(*) AS cleared_revision_count, "
                f"{flag_expressions} "
                f"FROM {DBT_SCHEMA}.fct_cleared_permit_revision"
            )
            values = result.fetchone()
            columns = [item[0] for item in result.description]
            totals = dict(zip(columns, values, strict=True))
            cleared_revision_count = int(totals.pop("cleared_revision_count") or 0)
            cleared_flag_counts = {
                key: int(value or 0) for key, value in totals.items()
            }

            # The refresh timestamp is written by CD only after a successful
            # dbt build/test run, then published as part of this DuckDB release.
            last_successful_refresh_utc = None
            refresh_metadata_relation = ("metadata", "dashboard_refresh_metadata")
            if refresh_metadata_relation in available:
                refresh_row = connection.execute(
                    "SELECT last_successful_refresh_utc "
                    "FROM metadata.dashboard_refresh_metadata WHERE refresh_id = 1"
                ).fetchone()
                if refresh_row:
                    last_successful_refresh_utc = refresh_row[0]
        finally:
            connection.close()

    if active.empty:
        raise RuntimeError(f"{DBT_SCHEMA}.mart_active_permit_age returned no records.")
    if summary.empty:
        raise RuntimeError(
            f"{DBT_SCHEMA}.mart_cleared_lifecycle_by_year_type returned no records."
        )
    return (
        active,
        summary,
        cleared_revision_count,
        cleared_flag_counts,
        last_successful_refresh_utc,
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


def report_sidebar(backend: str) -> tuple[str, Any, Any]:
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
        if backend == "snowflake":
            st.caption(
                "Reads the dashboard-facing dbt marts in TORONTO.ANALYTICS "
                "using the configured Snowflake role."
            )
        else:
            st.markdown(
                f"[Latest published DuckDB database on GitHub ↗]({RELEASE_PAGE_URL})"
            )
            st.caption(
                "The dashboard uses Toronto Open Data processed by this project's daily pipeline."
            )
        last_successful_refresh_slot = st.empty()
        if st.button(
            "Refresh data now",
            key="refresh_data_button",
            width="stretch",
            help=(
                "Re-query the Snowflake dashboard marts."
                if backend == "snowflake"
                else "Fetch the latest published database from GitHub. "
                "The file is about 120 MB."
            ),
        ):
            load_dashboard_data.clear()
            st.session_state["refresh_published_data"] = True
            st.rerun()
        loading_slot = st.empty()
        st.caption(
            "Data is cached for up to 24 hours. Refreshing downloads the database again."
        )
        st.divider()
        st.caption(
            "These charts show patterns in the data. They do not explain what caused a permit delay."
        )
        st.markdown(
            "[Project repository ↗](https://github.com/gagadanalyst/toronto-housing-bottleneck-analytics)"
        )
    return page, loading_slot, last_successful_refresh_slot


def overview(active: pd.DataFrame, summary: pd.DataFrame, backend: str) -> None:
    hero(
        "TORONTO OPEN DATA · BUILDING PERMITS",
        "What can permit records tell us about time?",
        "Explore how long current Active records have been open, compare historical timelines for permits marked Closed, and see where the source data needs a closer look.",
    )

    active["is_age_eligible"] = normalize_bool(active["is_age_eligible"])
    eligible = active[active["is_age_eligible"]].copy()
    eligible_ages = number(eligible["age_since_application_days"]).dropna()
    total_active = len(active)
    eligible_count = len(eligible)
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

    summary["eligible_date_pairs"] = number(summary["eligible_date_pairs"])
    summary["median_days"] = number(summary["median_days"])
    summary["p90_days"] = number(summary["p90_days"])
    years = number(summary["application_year"]).dropna()
    year_range = (
        f"{int(years.min())}–{int(years.max())}" if not years.empty else "Not available"
    )
    backend_label = (
        "Snowflake warehouse" if backend == "snowflake" else "Published DuckDB snapshot"
    )

    # Compact provenance row: only show metadata supported by the loaded data/config.
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
            "Active permit records",
            fmt_int(total_active),
            "Currently marked Active",
        )
    with cols[1]:
        metric_card(
            "Eligible for age analysis",
            fmt_int(eligible_count),
            f"{eligible_count / total_active:.1%} of Active records"
            if total_active
            else "No records",
        )
    with cols[2]:
        metric_card(
            "Median active age",
            fmt_days(eligible_ages.median()),
            "Elapsed time so far for eligible records",
        )
    with cols[3]:
        metric_card(
            "90th-percentile active age",
            fmt_days(eligible_ages.quantile(0.9)),
            "9 in 10 eligible records are at or below this age",
        )

    st.write("")
    left, right = st.columns([1, 1], gap="large")
    with left:
        st.subheader("What's still open?")
        st.markdown(
            '<div class="section-note">The age profile of records currently marked Active. '
            "These are ongoing timelines, not estimates of how long permits will take to finish.</div>",
            unsafe_allow_html=True,
        )
        if not eligible_ages.empty:
            hist = pd.DataFrame({"Age (days)": eligible_ages})
            fig = px.histogram(
                hist,
                x="Age (days)",
                nbins=30,
                color_discrete_sequence=[TEAL],
                title="Age of eligible Active permit records",
            )
            fig.update_traces(
                hovertemplate="Age: %{x:.0f} days<br>Records: %{y:,}<extra></extra>"
            )
            fig.update_xaxes(title="Days since application")
            fig.update_yaxes(title="Permit records")
            st.plotly_chart(chart_layout(fig, 410), width="stretch")
        else:
            st.info("No eligible Active records have a usable age value.")

    with right:
        st.subheader("What do closed records show?")
        st.markdown(
            f'<div class="section-note">Historical intervals for application years {year_range}. '
            "The bars compare typical and higher-end values within year/type groups.</div>",
            unsafe_allow_html=True,
        )
        if not summary.empty:
            measures = summary.groupby("measure", as_index=False).agg(
                typical_time=("median_days", "median"),
                higher_end_time=("p90_days", "median"),
            )
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    name="Median of group medians",
                    x=measures["measure"],
                    y=measures["typical_time"],
                    marker_color=TEAL,
                )
            )
            fig.add_trace(
                go.Bar(
                    name="Median of group 90th percentiles",
                    x=measures["measure"],
                    y=measures["higher_end_time"],
                    marker_color=RED,
                )
            )
            fig.update_layout(
                barmode="group",
                title="Typical and higher-end times",
            )
            fig.update_xaxes(title="Date interval")
            fig.update_yaxes(title="Days")
            st.plotly_chart(chart_layout(fig, 410), width="stretch")
        else:
            st.info("No closed-permit lifecycle summaries are available.")

    st.markdown(
        '<div class="callout"><b>How to read this:</b> Active ages are elapsed time so far. '
        "Closed-permit summaries describe historical records and compare year/type groups; "
        "they are not pooled permit-level averages and do not identify causes of delay.</div>",
        unsafe_allow_html=True,
    )

    st.caption(
        "The latest application year may be incomplete. Records with missing or conflicting "
        "information are handled by the documented eligibility and data-quality rules."
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
    st.subheader("Active permit records included in this view")
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
                name="Median of group medians",
                line=dict(color=TEAL, width=3),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=trend["application_year"],
                y=trend["p90_days"],
                mode="lines+markers",
                name="Median of group 90th percentiles",
                line=dict(color=RED, width=3),
            )
        )
        fig.update_layout(title="How times have changed by application year")
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
    st.subheader("Detailed results by year and permit type")
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
    cleared_flag_counts: dict[str, int],
    summary: pd.DataFrame,
) -> None:
    hero(
        "CHECK THE DATA",
        "Data quality and definitions",
        "See why some records are left out and where source information is missing or inconsistent.",
    )
    active = active.copy()
    for field in [
        "is_age_eligible",
        "has_status_conflict",
        "application_date_conflict",
        "issued_date_conflict",
        "geo_id_conflict",
    ]:
        if field in active:
            active[field] = normalize_bool(active[field])
    eligible_n = (
        int(active["is_age_eligible"].sum()) if "is_age_eligible" in active else 0
    )
    active_n = len(active)
    exclusion = (
        active.get("cohort_exclusion_reason", pd.Series(dtype=str))
        .fillna("included")
        .value_counts()
        .rename_axis("Reason")
        .reset_index(name="Records")
    )
    exclusion_labels = {
        "included": "Included in age analysis",
        "status_conflict": "Status information conflicts",
        "missing_status": "Status is missing",
        "terminal_or_exception_status": "Closed or special status",
        "application_date_conflict": "Application dates conflict",
        "missing_application_date": "Application date is missing",
    }
    exclusion["Reason"] = (
        exclusion["Reason"].map(exclusion_labels).fillna(exclusion["Reason"])
    )
    cols = st.columns(4)
    with cols[0]:
        metric_card(
            "Active permit records", fmt_int(active_n), "In the latest published data"
        )
    with cols[1]:
        metric_card(
            "Records included in age view",
            fmt_int(eligible_n),
            f"{eligible_n / active_n:.2%} included" if active_n else "No records",
        )
    with cols[2]:
        metric_card(
            "Records in Cleared data",
            fmt_int(cleared_revision_count),
            "Includes all statuses in the Cleared dataset",
        )
    with cols[3]:
        metric_card(
            "Closed-permit summary groups",
            fmt_int(len(summary)),
            "Grouped by year, type and date pair",
        )
    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Why some Active records are left out")
        fig = px.bar(
            exclusion,
            x="Records",
            y="Reason",
            orientation="h",
            color="Reason",
            color_discrete_sequence=[TEAL, GOLD, BLUE, RED],
            title="Records by reason",
        )
        fig.update_xaxes(title="Records")
        fig.update_yaxes(title="")
        st.plotly_chart(chart_layout(fig, 330), width="stretch")
    with right:
        st.subheader("Checks on the Cleared dataset")
        flag_fields = {
            "Builder names disagree": "builder_name_conflict",
            "Status values disagree": "has_status_conflict",
            "Application dates disagree": "application_date_conflict",
            "Issue dates disagree": "issued_date_conflict",
            "Completion dates disagree": "completed_date_conflict",
            "Geographic IDs disagree": "geo_id_conflict",
            "Issue date is before application": "application_to_issue_negative",
            "Completion date is before application": "application_to_completion_negative",
            "Completion date is before issue": "issue_to_completion_negative",
        }
        vals = []
        for label, field in flag_fields.items():
            vals.append(
                {
                    "Check": label,
                    "Records flagged": int(cleared_flag_counts.get(field, 0)),
                }
            )
        flag_df = pd.DataFrame(vals).sort_values("Records flagged")
        fig = px.bar(
            flag_df,
            x="Records flagged",
            y="Check",
            orientation="h",
            color="Records flagged",
            color_continuous_scale=[SCALE_LOW, TEAL],
            title="Records with each issue",
        )
        fig.update_xaxes(title="Records flagged")
        fig.update_yaxes(title="")
        st.plotly_chart(chart_layout(fig, 390), width="stretch")
    st.subheader("What the numbers mean")
    st.markdown("""
    - **Age of an Active permit:** calendar days from its application date to the date shown on the Active permits page. The permit is still open, so this is not a final completion time.
    - **Time between dates for a Closed permit:** calendar days between the selected dates for a record marked Closed. Records where dates run backwards are flagged and left out of the published duration statistics.
    - **Middle value (median):** half the values are below it and half are above it.
    - **90th percentile:** 9 out of 10 values are at or below this value. It helps show the higher end of the times.
    - **Group summaries:** some charts summarize the median or 90th percentile within each year and permit type, then compare those group values. They are not a single calculation across every permit.
    - **Data conflicts:** more than one current source row can exist for a permit revision, and values may disagree. Those disagreements are counted rather than silently choosing one value.
    """)
    st.subheader("What to keep in mind")
    st.markdown("""
    - Toronto's published data changes over time, so counts and results can change after a new release.
    - The latest application year is incomplete; compare it carefully with earlier years.
    - Some records have missing dates, unusual statuses, repeated rows or conflicting details.
    - The dashboard uses the latest published database from GitHub, not a live query to the City website. Data is cached for up to 24 hours, unless **Refresh data now** is selected.
    - These charts describe what appears in the data. They do not show why a permit took longer.
    """)


def main() -> None:
    try:
        backend = selected_dashboard_backend()
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
        st.error("The dashboard backend configuration could not be loaded.")
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        st.stop()

    page, loading_slot, last_successful_refresh_slot = report_sidebar(backend)
    force_refresh = bool(st.session_state.pop("refresh_published_data", False))
    last_loaded_at = float(st.session_state.get("_dashboard_loaded_at", 0.0))
    cache_may_have_expired = (time() - last_loaded_at) >= CACHE_TTL_SECONDS
    backend_key = (backend, config_fingerprint)
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
                        cleared_count,
                        cleared_flag_counts,
                        last_successful_refresh_utc,
                    ) = load_dashboard_data(backend, config_fingerprint)
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
                cleared_count,
                cleared_flag_counts,
                last_successful_refresh_utc,
            ) = load_dashboard_data(backend, config_fingerprint)
        st.session_state["_dashboard_loaded"] = True
        st.session_state["_dashboard_backend_key"] = backend_key
        if show_loading_status:
            st.session_state["_dashboard_loaded_at"] = time()
    except Exception as exc:
        st.error("The dashboard could not load the selected backend's permit data.")
        if backend == "duckdb":
            st.markdown(
                "Check your connection and try **Refresh data now**. The app downloads "
                "the latest published database from the public "
                f"[GitHub release]({RELEASE_PAGE_URL})."
            )
        else:
            st.markdown(
                "Check the Snowflake connection settings, the dashboard role's "
                "read-only grants, and whether all three required dbt relations "
                "exist in the configured analytics schema. The app will not silently "
                "switch to DuckDB when Snowflake fails."
            )
        with st.expander("Technical details"):
            st.code(f"{type(exc).__name__}: {exc}")
        st.stop()

    last_refresh_display = format_refresh_timestamp(last_successful_refresh_utc)
    with last_successful_refresh_slot.container():
        st.divider()
        st.markdown(
            f"""<div style="padding-top:.15rem;padding-bottom:.65rem;">
              <div style="font-size:.70rem;font-weight:750;letter-spacing:.10em;
                          color:#C0CAD3;margin-bottom:.55rem;">
                LAST SUCCESSFUL DATA REFRESH
              </div>
              <div style="display:flex;align-items:center;gap:.55rem;">
                <svg xmlns="http://www.w3.org/2000/svg" width="21" height="21"
                     viewBox="0 0 24 24" fill="none" stroke="#21B6E8"
                     stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
                     aria-hidden="true" style="flex:0 0 auto;">
                  <rect x="3" y="4" width="18" height="17" rx="2"></rect>
                  <path d="M8 2v4M16 2v4M3 10h12"></path>
                  <circle cx="17" cy="17" r="4"></circle>
                  <path d="M17 15v2l1.3 1"></path>
                </svg>
                <span style="font-weight:700;color:#F4F7FA;font-size:.92rem;
                             line-height:1.35;">Date and time of completed refresh</span>
              </div>
              <div style="font-weight:750;color:#F4F7FA;font-size:1rem;
                          line-height:1.35;margin-top:.4rem;">{last_refresh_display}</div>
              <div style="font-size:.75rem;color:#C0CAD3;margin-top:.4rem;">
                Displayed in Toronto local time (ET).
              </div>
            </div>""",
            unsafe_allow_html=True,
        )

    if page == "The story":
        overview(active.copy(), summary.copy(), backend)
    elif page == "Active permits":
        active_page(active.copy())
    elif page == "Closed-permit timelines":
        lifecycle_page(summary.copy())
    else:
        quality_page(active.copy(), cleared_count, cleared_flag_counts, summary.copy())

    st.divider()
    st.caption(
        "Toronto Housing Bottleneck Analytics · Source: City of Toronto Open Data · "
        "Updated through the project's published data pipeline · Patterns shown are descriptive, not causal"
    )


if __name__ == "__main__":
    main()
