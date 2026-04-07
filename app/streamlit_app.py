"""
Tech Ecosystem Tracker — Streamlit Dashboard
Runs locally (snowflake.connector + .env) and in Streamlit in Snowflake (Snowpark session).
Local:    streamlit run app/streamlit_app.py
Snowflake: deploy via snowflake/04_streamlit_setup.sql
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Environment detection ─────────────────────────────────────────────────────
# Snowflake injects an active Snowpark session; locally we use connector + .env.
try:
    from snowflake.snowpark.context import get_active_session
    _SF_SESSION = get_active_session()
    _IN_SNOWFLAKE = True
except Exception:
    _SF_SESSION = None
    _IN_SNOWFLAKE = False

if not _IN_SNOWFLAKE:
    from dotenv import load_dotenv
    import snowflake.connector
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ──────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG  (must be first Streamlit call)
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tech Ecosystem Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# GLOBAL STYLES
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    /* Top header bar */
    [data-testid="stHeader"] { background: #0E1117; }

    /* Sidebar container */
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] > div:first-child {
        background-color: #161C27 !important;
        border-right: 1px solid #2A3347;
    }

    /* All sidebar text white */
    [data-testid="stSidebar"],
    [data-testid="stSidebar"] *,
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] div {
        color: #FAFAFA !important;
    }

    /* Sidebar headings */
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #00D4FF !important;
    }

    /* Radio buttons (page nav) */
    [data-testid="stSidebar"] [data-testid="stRadio"] label,
    [data-testid="stSidebar"] [data-testid="stRadio"] div,
    [data-testid="stSidebar"] [data-testid="stRadio"] p {
        color: #FAFAFA !important;
    }
    [data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
        background: rgba(255,255,255,0.08) !important;
        border-radius: 6px;
    }

    /* Sidebar nav links (multi-page apps) */
    [data-testid="stSidebarNav"] a {
        color: #FAFAFA !important;
        text-decoration: none !important;
        padding: 0.5rem 1rem !important;
        border-radius: 6px !important;
        display: block !important;
        transition: background 0.15s ease;
    }
    [data-testid="stSidebarNav"] a:hover {
        background: rgba(255,255,255,0.1) !important;
    }
    [data-testid="stSidebarNav"] a[aria-current="page"] {
        background: #0E4C92 !important;
        border-left: 4px solid #29B5E8 !important;
        color: #FFFFFF !important;
    }
    [data-testid="stSidebarNav"] a span {
        color: #FAFAFA !important;
    }

    /* SVG icons */
    [data-testid="stSidebar"] svg {
        fill: #FAFAFA !important;
        color: #FAFAFA !important;
    }

    /* Page title */
    .page-title {
        font-size: 1.8rem;
        font-weight: 700;
        color: #00D4FF;
        margin-bottom: 0.25rem;
    }
    .page-subtitle {
        color: #8B9CB0;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    /* Metric cards */
    [data-testid="stMetric"] {
        background: #161C27;
        border: 1px solid #2A3347;
        border-radius: 8px;
        padding: 1rem;
    }
    [data-testid="stMetricLabel"] { color: #8B9CB0 !important; }
    [data-testid="stMetricValue"] { color: #FAFAFA !important; }

    /* AI insight box */
    .insight-box {
        background: linear-gradient(135deg, #0D1F33 0%, #162840 100%);
        border: 1px solid #00D4FF44;
        border-left: 3px solid #00D4FF;
        border-radius: 8px;
        padding: 1rem 1.25rem;
        margin-top: 1.5rem;
    }
    .insight-box .label {
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        color: #00D4FF;
        text-transform: uppercase;
        margin-bottom: 0.5rem;
    }
    .insight-box p { color: #C8D6E0; margin: 0; line-height: 1.6; }

    /* Gap badges */
    .badge-over  { background:#FF6B6B22; color:#FF6B6B; border:1px solid #FF6B6B55;
                   border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .badge-under { background:#4ECDC422; color:#4ECDC4; border:1px solid #4ECDC455;
                   border-radius:4px; padding:2px 8px; font-size:0.8rem; }
    .badge-ok    { background:#A8E6CF22; color:#A8E6CF; border:1px solid #A8E6CF55;
                   border-radius:4px; padding:2px 8px; font-size:0.8rem; }

    /* Divider */
    hr { border-color: #2A3347; }

    /* Section header */
    .section-header {
        font-size: 1rem;
        font-weight: 600;
        color: #8B9CB0;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin: 1.5rem 0 0.75rem 0;
    }

    /* ── Main app background & base text ──────────────────────────────────── */
    .stApp, [data-testid="stAppViewContainer"] {
        background-color: #0E1117 !important;
        color: #FAFAFA !important;
    }

    /* ── All labels, text, headings in main content ────────────────────────── */
    p, span, div, h1, h2, h3, h4, h5, h6 { color: #FAFAFA !important; }
    label { color: #FAFAFA !important; }
    small, .stCaption { color: #B0B0B0 !important; }

    /* ── Radio buttons (main content) ──────────────────────────────────────── */
    [role="radiogroup"] label,
    [role="radiogroup"] label > div,
    [role="radiogroup"] label p,
    [role="radiogroup"] label span,
    [data-testid="stRadio"] label,
    [data-testid="stRadio"] label p,
    [data-testid="stRadio"] label span { color: #FAFAFA !important; }

    /* ── Checkboxes, selectboxes, sliders, text inputs ─────────────────────── */
    [data-testid="stCheckbox"] label,
    [data-testid="stSelectbox"] label,
    [data-testid="stMultiSelect"] label,
    [data-testid="stSlider"] label,
    [data-testid="stNumberInput"] label,
    [data-testid="stTextInput"] label,
    [data-testid="stWidgetLabel"],
    [data-testid="stWidgetLabel"] p { color: #FAFAFA !important; }

    /* ── Selectbox dropdown text ────────────────────────────────────────────── */
    [data-baseweb="select"] * { color: #FAFAFA !important; }
    [data-baseweb="select"] [data-baseweb="input"] { color: #FAFAFA !important; }

    /* ── Expanders ──────────────────────────────────────────────────────────── */
    [data-testid="stExpander"] summary,
    [data-testid="stExpander"] summary p,
    [data-testid="stExpander"] summary span { color: #FAFAFA !important; }
    [data-testid="stExpander"] summary {
        background-color: rgba(255,255,255,0.04) !important;
    }

    /* ── Dataframe ──────────────────────────────────────────────────────────── */
    [data-testid="stDataFrame"] th { color: #FFFFFF !important; background: rgba(255,255,255,0.1) !important; }
    [data-testid="stDataFrame"] td { color: #FAFAFA !important; }

    /* ── Plotly chart text override ─────────────────────────────────────────── */
    .js-plotly-plot .plotly text { fill: #FAFAFA !important; }
    [data-testid="stPlotlyChart"] { background: rgba(255,255,255,0.02); border-radius: 8px; }

    /* ── Tabs ───────────────────────────────────────────────────────────────── */
    [data-baseweb="tab-list"] button { color: #B0B0B0 !important; }
    [data-baseweb="tab-list"] button[aria-selected="true"] {
        color: #29B5E8 !important;
        border-bottom: 2px solid #29B5E8 !important;
    }

    /* ── Code ───────────────────────────────────────────────────────────────── */
    code { color: #29B5E8 !important; background: rgba(255,255,255,0.1) !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# SNOWFLAKE  (Snowpark session in SiS, connector locally)
# ──────────────────────────────────────────────────────────────────────────────
if not _IN_SNOWFLAKE:
    _SF = dict(
        account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        user=os.environ.get("SNOWFLAKE_USER", ""),
        password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        database="TECH_ECOSYSTEM",
        schema="RAW_ANALYTICS",
        warehouse="TECH_WH",
        role="SYSADMIN",
    )


@st.cache_data(ttl=3600, show_spinner="Querying Snowflake…")
def query(sql: str) -> pd.DataFrame:
    if _IN_SNOWFLAKE:
        return _SF_SESSION.sql(sql).to_pandas().rename(columns=str.lower)
    conn = snowflake.connector.connect(**_SF)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# DATA ROLES SKILLS — CSVs locally, raw_stackoverflow_survey in Snowflake
# ──────────────────────────────────────────────────────────────────────────────

# Role name normalization across survey years
_ROLE_ALIASES = {
    "Data engineer": ["Data engineer", "Engineer, data"],
    "Data scientist / ML": ["Data scientist or machine learning specialist", "Data scientist", "AI/ML engineer"],
    "Data / Business analyst": ["Data or business analyst"],
    "Database administrator": ["Database administrator", "Database administrator or engineer"],
}

# CSV column → Snowflake column mapping
_SKILL_COLS = {
    "Languages":             ("LanguageHaveWorkedWith",     "language_have_worked_with"),
    "Databases":             ("DatabaseHaveWorkedWith",     "database_have_worked_with"),
    "Cloud Platforms":       ("PlatformHaveWorkedWith",     "platform_have_worked_with"),
    "Libraries & Frameworks":("MiscTechHaveWorkedWith",     "misc_tech_have_worked_with"),
    "Dev Tools":             ("ToolsTechHaveWorkedWith",     "tools_tech_have_worked_with"),
    "IDEs & Editors":        ("NEWCollabToolsHaveWorkedWith","collab_tools_have_worked_with"),
}

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "stackoverflow")


@st.cache_data(show_spinner="Loading survey data…")
def load_data_roles_skills(year: int) -> pd.DataFrame:
    if _IN_SNOWFLAKE:
        all_aliases = []
        for aliases in _ROLE_ALIASES.values():
            all_aliases.extend(aliases)
        like_clauses = " OR ".join(
            f"LOWER(dev_type) LIKE '%{a.lower()}%'" for a in all_aliases
        )
        sql = f"""
            SELECT
                survey_year                   AS yr,
                dev_type                      AS devtype,
                language_have_worked_with     AS lang,
                database_have_worked_with     AS db,
                platform_have_worked_with     AS platform,
                misc_tech_have_worked_with    AS misc,
                tools_tech_have_worked_with   AS tools,
                collab_tools_have_worked_with AS collab
            FROM TECH_ECOSYSTEM.RAW.raw_stackoverflow_survey
            WHERE survey_year = {year}
              AND ({like_clauses})
        """
        df = query(sql)
        df = df.rename(columns={
            "yr":       "year",
            "devtype":  "DevType",
            "lang":     "LanguageHaveWorkedWith",
            "db":       "DatabaseHaveWorkedWith",
            "platform": "PlatformHaveWorkedWith",
            "misc":     "MiscTechHaveWorkedWith",
            "tools":    "ToolsTechHaveWorkedWith",
            "collab":   "NEWCollabToolsHaveWorkedWith",
        })
    else:
        path = os.path.join(_DATA_DIR, f"survey_{year}.csv")
        df = pd.read_csv(path, low_memory=False)
        df["year"] = year

    df["DevType"] = df["DevType"].fillna("")

    rows = []
    for canonical_role, aliases in _ROLE_ALIASES.items():
        pattern = "|".join(aliases)
        subset = df[df["DevType"].str.contains(pattern, case=False, na=False)].copy()
        subset["role"] = canonical_role
        rows.append(subset)

    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def top_skills_for_role(df: pd.DataFrame, skill_col: str, top_n: int = 15) -> pd.DataFrame:
    col = _SKILL_COLS[skill_col][0]  # always use CSV column name (both paths use it)
    if col not in df.columns:
        return pd.DataFrame(columns=["skill", "count", "pct"])
    total = df[col].notna().sum()
    if total == 0:
        return pd.DataFrame(columns=["skill", "count", "pct"])
    counts = (
        df[col].dropna()
        .str.split(";")
        .explode()
        .str.strip()
        .value_counts()
        .head(top_n)
        .reset_index()
    )
    counts.columns = ["skill", "count"]
    counts["pct"] = (counts["count"] / total * 100).round(1)
    return counts


# ──────────────────────────────────────────────────────────────────────────────
# AI INSIGHTS
# In Snowflake: Snowflake Cortex (no API key / network rules needed)
# Locally:      Anthropic API via ANTHROPIC_API_KEY env var
# ──────────────────────────────────────────────────────────────────────────────
def generate_insight(context: str, prompt_prefix: str = "") -> str:
    default_prefix = (
        "You are a developer trends analyst. "
        "Write exactly 3 concise, data-grounded sentences. "
        "Cite specific numbers and year-over-year changes where available. "
        "Be direct — no preamble.\n\n"
        f"Data snapshot:\n"
    )
    full_prompt = (prompt_prefix or default_prefix) + context

    if _IN_SNOWFLAKE:
        # Snowflake Cortex — works on all account types including trial
        try:
            escaped = full_prompt.replace("'", "\\'")
            row = _SF_SESSION.sql(
                f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', '{escaped}') AS response"
            ).collect()
            return row[0]["RESPONSE"].strip() if row else "No response from Cortex."
        except Exception as exc:
            return f"Cortex error: {exc}"
    else:
        # Local: use Anthropic API
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return "Set ANTHROPIC_API_KEY in .env to enable AI insights locally."
        try:
            import anthropic as _anthropic
            client = _anthropic.Anthropic(api_key=api_key)
            msg = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=220,
                messages=[{"role": "user", "content": full_prompt}],
            )
            return msg.content[0].text.strip()
        except Exception as exc:
            return f"Could not generate insight: {exc}"


# ──────────────────────────────────────────────────────────────────────────────
# CHART DEFAULTS
# ──────────────────────────────────────────────────────────────────────────────
TMPL = "plotly_dark"
CHART_BG = "#0E1117"
PAPER_BG = "#0E1117"
GRID_COLOR = "#2A3347"

CHART_LAYOUT = dict(
    template=TMPL,
    plot_bgcolor=CHART_BG,
    paper_bgcolor=PAPER_BG,
    font=dict(color="#FAFAFA", size=12),
    font_color="#FAFAFA",
    title_font=dict(color="#FFFFFF", size=15),
    xaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR, color="#FAFAFA", title_font=dict(color="#FAFAFA")),
    yaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR, color="#FAFAFA", title_font=dict(color="#FAFAFA")),
    margin=dict(l=10, r=10, t=40, b=10),
    legend=dict(bgcolor="#161C27", bordercolor="#2A3347", borderwidth=1, font=dict(color="#FAFAFA")),
)

COLOR_SEQ = px.colors.qualitative.Vivid


# ──────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📊 Tech Ecosystem\n### Tracker")
    st.markdown("---")

    page = st.radio(
        "page",
        [
            "📈  Technology Adoption",
            "🔍  Said vs Did",
            "💰  Developer Economics",
            "🧑‍💻  Data Roles Skills",
        ],
        label_visibility="collapsed",
    )

    st.markdown("---")
    st.markdown(
        """
**Data Sources**
- 🟡 Stack Overflow Survey
- 🔵 JetBrains Dev Survey
- 🟢 GitHub Archive

**Coverage:** 2021 – 2025

**Pipeline**
`dbt` → Snowflake → Streamlit
        """
    )
    st.markdown("---")
    st.caption("Powered by Claude · Anthropic API")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — TECHNOLOGY ADOPTION TRENDS
# ══════════════════════════════════════════════════════════════════════════════
if page == "📈  Technology Adoption":
    st.markdown('<div class="page-title">Technology Adoption Trends</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">Survey adoption % for the top 15 technologies · 2021–2025</div>',
        unsafe_allow_html=True,
    )

    # ── LOAD DATA ────────────────────────────────────────────────────────────
    df_raw = query(
        """
        SELECT
            year,
            technology_name,
            technology_category,
            data_source,
            survey_adoption_pct
        FROM TECH_ECOSYSTEM.RAW_ANALYTICS.fct_technology_adoption
        WHERE year BETWEEN 2021 AND 2025
          AND data_source IN ('stackoverflow', 'jetbrains')
          AND survey_adoption_pct IS NOT NULL
        ORDER BY year, technology_name
        """
    )

    if df_raw.empty:
        st.warning("No data returned from FCT_TECHNOLOGY_ADOPTION. Check your Snowflake connection.")
        st.stop()

    # ── FILTERS ──────────────────────────────────────────────────────────────
    col_a, col_b, col_c = st.columns([2, 2, 2])

    categories = ["All"] + sorted(df_raw["technology_category"].dropna().unique().tolist())
    with col_a:
        selected_cat = st.selectbox("Technology Category", categories)

    with col_b:
        source_choice = st.radio(
            "Data Source",
            ["Combined Average", "Stack Overflow Only", "JetBrains Only"],
            horizontal=True,
        )

    with col_c:
        top_n = st.slider("Top N technologies", min_value=5, max_value=25, value=15, step=5)

    st.markdown("---")

    # ── FILTER & AGGREGATE ────────────────────────────────────────────────────
    df = df_raw.copy()

    if selected_cat != "All":
        df = df[df["technology_category"] == selected_cat]

    if source_choice == "Stack Overflow Only":
        df = df[df["data_source"] == "stackoverflow"]
    elif source_choice == "JetBrains Only":
        df = df[df["data_source"] == "jetbrains"]
    else:
        # Average SO + JB per (year, technology)
        df = (
            df.groupby(["year", "technology_name", "technology_category"])["survey_adoption_pct"]
            .mean()
            .reset_index()
        )

    # Top N by overall average adoption
    top_techs = (
        df.groupby("technology_name")["survey_adoption_pct"]
        .mean()
        .nlargest(top_n)
        .index.tolist()
    )
    df = df[df["technology_name"].isin(top_techs)]

    if df.empty:
        st.info("No data for the selected filters.")
        st.stop()

    # ── SUMMARY METRICS ───────────────────────────────────────────────────────
    latest_year = df["year"].max()
    prev_year = latest_year - 1
    latest = df[df["year"] == latest_year].groupby("technology_name")["survey_adoption_pct"].mean()
    prev = df[df["year"] == prev_year].groupby("technology_name")["survey_adoption_pct"].mean()

    top_tech_latest = latest.idxmax() if not latest.empty else "—"
    top_pct = latest.max() if not latest.empty else 0.0
    yoy_delta = (
        round(latest.get(top_tech_latest, 0) - prev.get(top_tech_latest, 0), 1)
        if top_tech_latest in prev
        else None
    )

    m1, m2, m3 = st.columns(3)
    m1.metric("Top Technology", top_tech_latest, f"{top_pct:.1f}% adoption")
    m2.metric(
        f"YoY Change ({prev_year}→{latest_year})",
        f"{yoy_delta:+.1f} pts" if yoy_delta is not None else "—",
    )
    m3.metric("Technologies Shown", len(top_techs))

    # ── LINE CHART ────────────────────────────────────────────────────────────
    fig = px.line(
        df.sort_values("year"),
        x="year",
        y="survey_adoption_pct",
        color="technology_name",
        markers=True,
        labels={
            "survey_adoption_pct": "Adoption %",
            "year": "Year",
            "technology_name": "Technology",
        },
        color_discrete_sequence=COLOR_SEQ,
    )
    fig.update_traces(line_width=2.5, marker_size=7)
    fig.update_layout(
        **CHART_LAYOUT,
        height=520,
        title=dict(
            text=f"Top {top_n} Technologies · {source_choice}",
            font_size=15,
            x=0,
        ),
        hovermode="x unified",
    )
    fig.update_xaxes(tickmode="linear", tick0=2021, dtick=1, gridcolor=GRID_COLOR)
    fig.update_yaxes(ticksuffix="%", gridcolor=GRID_COLOR)
    st.plotly_chart(fig, use_container_width=True)

    # ── AI NARRATIVE INSIGHT ──────────────────────────────────────────────────
    st.markdown('<div class="section-header">🤖 AI Insight</div>', unsafe_allow_html=True)

    # Build context for Claude
    pivot = df.pivot_table(
        index="technology_name", columns="year", values="survey_adoption_pct", aggfunc="mean"
    ).round(1)

    years_available = sorted(df["year"].unique())
    context_lines = []
    for tech in top_techs[:10]:
        if tech not in pivot.index:
            continue
        row = pivot.loc[tech]
        trend = []
        for yr in years_available:
            if yr in row and pd.notna(row[yr]):
                trend.append(f"{yr}: {row[yr]:.1f}%")
        if trend:
            context_lines.append(f"{tech}: {', '.join(trend)}")

    context_str = (
        f"Category filter: {selected_cat}\n"
        f"Source: {source_choice}\n"
        f"Top tech this year ({latest_year}): {top_tech_latest} at {top_pct:.1f}%\n\n"
        + "\n".join(context_lines)
    )

    if st.button("✨ Generate AI Insight", key="btn_insight"):
        with st.spinner("Asking Claude…"):
            insight = generate_insight(context_str)
        st.markdown(
            f"""
            <div class="insight-box">
              <div class="label">AI Narrative · Claude</div>
              <p>{insight}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── RAW DATA EXPANDER ─────────────────────────────────────────────────────
    with st.expander("View raw data"):
        st.dataframe(
            df.sort_values(["year", "survey_adoption_pct"], ascending=[True, False]),
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — SAID VS DID
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔍  Said vs Did":
    st.markdown('<div class="page-title">Said vs Did</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">'
        "Survey adoption % vs actual GitHub activity — where does developer self-reporting diverge from reality?"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── LOAD DATA ────────────────────────────────────────────────────────────
    df_svd = query(
        """
        SELECT
            year,
            technology_name,
            technology_category,
            avg_survey_adoption_pct,
            github_activity_pct,
            said_vs_did_gap,
            said_vs_did_classification
        FROM TECH_ECOSYSTEM.RAW_ANALYTICS.fct_said_vs_did
        WHERE avg_survey_adoption_pct IS NOT NULL
           OR github_activity_pct IS NOT NULL
        ORDER BY year, abs(said_vs_did_gap) DESC NULLS LAST
        """
    )

    if df_svd.empty:
        st.warning("No data returned from FCT_SAID_VS_DID. Check your Snowflake connection.")
        st.stop()

    # ── FILTERS ──────────────────────────────────────────────────────────────
    col_a, col_b = st.columns([2, 3])

    years_available = sorted(df_svd["year"].dropna().unique().astype(int).tolist())
    with col_a:
        selected_year = st.select_slider(
            "Survey Year",
            options=years_available,
            value=max(years_available),
        )

    with col_b:
        show_n = st.slider("Technologies to show", min_value=10, max_value=40, value=25, step=5)

    st.markdown("---")

    df = df_svd[df_svd["year"] == selected_year].copy()
    df = df.dropna(subset=["avg_survey_adoption_pct"])
    df = df.sort_values("said_vs_did_gap", ascending=False).head(show_n)

    if df.empty:
        st.info(f"No data for {selected_year}.")
        st.stop()

    # ── SUMMARY METRICS ───────────────────────────────────────────────────────
    over = (df["said_vs_did_classification"] == "over_reported").sum()
    under = (df["said_vs_did_classification"] == "under_reported").sum()
    consistent = (df["said_vs_did_classification"] == "consistent").sum()
    max_gap_row = df.loc[df["said_vs_did_gap"].idxmax()]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Over-reported", int(over), help="Survey % far exceeds GitHub activity")
    m2.metric("Under-reported", int(under), help="More GitHub activity than surveys suggest")
    m3.metric("Consistent", int(consistent))
    m4.metric(
        "Biggest Hype Gap",
        max_gap_row["technology_name"],
        f"+{max_gap_row['said_vs_did_gap']:.1f} pts",
    )

    # ── GROUPED BAR CHART ────────────────────────────────────────────────────
    df_plot = df.copy()
    df_plot["github_activity_pct"] = df_plot["github_activity_pct"].fillna(0)

    # Color map for classification
    classification_colors = {
        "over_reported": "#FF6B6B",
        "under_reported": "#4ECDC4",
        "consistent": "#A8E6CF",
    }
    df_plot["bar_color"] = df_plot["said_vs_did_classification"].map(classification_colors).fillna("#8B9CB0")

    fig = go.Figure()

    # Survey bar
    fig.add_trace(
        go.Bar(
            name="Survey Adoption %",
            x=df_plot["technology_name"],
            y=df_plot["avg_survey_adoption_pct"],
            marker_color="#4A90D9",
            marker_opacity=0.9,
            hovertemplate="<b>%{x}</b><br>Survey: %{y:.1f}%<extra></extra>",
        )
    )

    # GitHub bar
    fig.add_trace(
        go.Bar(
            name="GitHub Activity %",
            x=df_plot["technology_name"],
            y=df_plot["github_activity_pct"],
            marker_color="#F5A623",
            marker_opacity=0.9,
            hovertemplate="<b>%{x}</b><br>GitHub: %{y:.1f}%<extra></extra>",
        )
    )

    fig.update_layout(
        **CHART_LAYOUT,
        barmode="group",
        height=520,
        title=dict(
            text=f"Survey vs GitHub Activity · {selected_year}  "
                 "(blue = survey claimed, orange = GitHub actual)",
            font_size=14,
            x=0,
        ),
        bargap=0.2,
        bargroupgap=0.05,
    )
    fig.update_xaxes(tickangle=-40, gridcolor=GRID_COLOR)
    fig.update_yaxes(ticksuffix="%", gridcolor=GRID_COLOR, title="Percentage")
    st.plotly_chart(fig, use_container_width=True)

    # ── GAP WATERFALL ────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Gap Analysis (Survey % − GitHub %)</div>', unsafe_allow_html=True)

    df_gap = df.sort_values("said_vs_did_gap", ascending=True)
    colors = [classification_colors.get(c, "#8B9CB0") for c in df_gap["said_vs_did_classification"]]

    fig2 = go.Figure(
        go.Bar(
            x=df_gap["said_vs_did_gap"],
            y=df_gap["technology_name"],
            orientation="h",
            marker_color=colors,
            hovertemplate="<b>%{y}</b><br>Gap: %{x:+.1f} pts<extra></extra>",
        )
    )
    fig2.update_layout(
        **CHART_LAYOUT,
        height=max(350, len(df_gap) * 22),
        title=dict(text="Said vs Did Gap (positive = over-reported)", font_size=14, x=0),
        showlegend=False,
    )
    fig2.update_xaxes(ticksuffix=" pts", gridcolor=GRID_COLOR, zeroline=True, zerolinecolor="rgba(255,255,255,0.267)")
    fig2.update_yaxes(gridcolor="rgba(0,0,0,0)")
    # Reference line at zero
    fig2.add_vline(x=0, line_color="rgba(255,255,255,0.333)", line_width=1)

    # Legend annotation
    fig2.add_annotation(
        x=0.99, y=1.05, xref="paper", yref="paper",
        text="🔴 Over-reported  🔵 Consistent  🟢 Under-reported",
        showarrow=False, font_size=11, font_color="#8B9CB0",
        xanchor="right",
    )
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("View raw data"):
        st.dataframe(
            df[["technology_name", "technology_category", "avg_survey_adoption_pct",
                "github_activity_pct", "said_vs_did_gap", "said_vs_did_classification"]]
            .sort_values("said_vs_did_gap", ascending=False),
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — DEVELOPER ECONOMICS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "💰  Developer Economics":
    st.markdown('<div class="page-title">Developer Economics</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">'
        "Median salary by technology · filtered by experience level and country · USD/year"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── LOAD DATA ────────────────────────────────────────────────────────────
    df_sal = query(
        """
        SELECT
            survey_year,
            technology_name,
            technology_category,
            country,
            years_coding_professional,
            respondent_count,
            median_salary,
            p25_salary,
            p75_salary,
            salary_premium_pct,
            global_median_salary
        FROM TECH_ECOSYSTEM.RAW_ANALYTICS.fct_salary_by_stack
        WHERE median_salary IS NOT NULL
          AND respondent_count >= 10
        ORDER BY survey_year DESC, median_salary DESC
        """
    )

    if df_sal.empty:
        st.warning("No data returned from FCT_SALARY_BY_STACK. Check your Snowflake connection.")
        st.stop()

    df_sal["survey_year"] = df_sal["survey_year"].astype(int)

    # ── FILTERS ──────────────────────────────────────────────────────────────
    col_a, col_b, col_c, col_d = st.columns([1.5, 2, 2, 1.5])

    years = sorted(df_sal["survey_year"].unique(), reverse=True)
    with col_a:
        sel_year = st.selectbox("Year", years)

    # Top countries by respondent count
    top_countries = (
        df_sal[df_sal["survey_year"] == sel_year]
        .groupby("country")["respondent_count"]
        .sum()
        .nlargest(30)
        .index.tolist()
    )
    with col_b:
        sel_country = st.selectbox("Country", ["All"] + top_countries)

    # Experience levels
    exp_levels = sorted(df_sal["years_coding_professional"].dropna().unique().tolist())
    with col_c:
        sel_exp = st.selectbox("Experience Level", ["All"] + exp_levels)

    with col_d:
        top_n_sal = st.slider("Top N techs", 10, 40, 20, 5)

    st.markdown("---")

    # ── FILTER ────────────────────────────────────────────────────────────────
    df = df_sal[df_sal["survey_year"] == sel_year].copy()
    if sel_country != "All":
        df = df[df["country"] == sel_country]
    if sel_exp != "All":
        df = df[df["years_coding_professional"] == sel_exp]

    # Aggregate across remaining dimensions
    df_agg = (
        df.groupby(["technology_name", "technology_category"])
        .agg(
            median_salary=("median_salary", "median"),
            p25_salary=("p25_salary", "median"),
            p75_salary=("p75_salary", "median"),
            salary_premium_pct=("salary_premium_pct", "mean"),
            respondent_count=("respondent_count", "sum"),
            global_median_salary=("global_median_salary", "mean"),
        )
        .reset_index()
        .sort_values("median_salary", ascending=False)
        .head(top_n_sal)
    )

    if df_agg.empty:
        st.info("No data for the selected filters. Try broadening the selection.")
        st.stop()

    # ── SUMMARY METRICS ───────────────────────────────────────────────────────
    global_med = df_agg["global_median_salary"].mean()
    top_row = df_agg.iloc[0]
    bottom_row = df_agg.iloc[-1]

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Global Median", f"${global_med:,.0f}")
    m2.metric(
        "Highest Paying",
        top_row["technology_name"],
        f"${top_row['median_salary']:,.0f}",
    )
    m3.metric(
        "Lowest in Top N",
        bottom_row["technology_name"],
        f"${bottom_row['median_salary']:,.0f}",
    )
    m4.metric("Technologies Shown", len(df_agg))

    # ── SALARY BAR CHART WITH IQR ─────────────────────────────────────────────
    df_chart = df_agg.sort_values("median_salary", ascending=True)

    # Color by premium: green = above global median, red = below
    bar_colors = [
        "#4ECDC4" if prem >= 0 else "#FF6B6B"
        for prem in df_chart["salary_premium_pct"]
    ]

    # Confidence opacity by respondent count (more respondents = more opaque)
    max_resp = df_chart["respondent_count"].max()
    opacities = [
        max(0.4, min(1.0, row["respondent_count"] / max_resp))
        for _, row in df_chart.iterrows()
    ]

    fig = go.Figure()

    # IQR error bars (p25→p75)
    fig.add_trace(
        go.Bar(
            x=df_chart["median_salary"],
            y=df_chart["technology_name"],
            orientation="h",
            marker=dict(
                color=bar_colors,
                opacity=opacities,
                line=dict(color="#2A3347", width=0.5),
            ),
            error_x=dict(
                type="data",
                symmetric=False,
                array=(df_chart["p75_salary"] - df_chart["median_salary"]).clip(lower=0),
                arrayminus=(df_chart["median_salary"] - df_chart["p25_salary"]).clip(lower=0),
                color="rgba(255,255,255,0.333)",
                thickness=2,
                width=4,
            ),
            customdata=df_chart[["p25_salary", "p75_salary", "salary_premium_pct", "respondent_count"]].values,
            hovertemplate=(
                "<b>%{y}</b><br>"
                "Median: $%{x:,.0f}<br>"
                "P25: $%{customdata[0]:,.0f} · P75: $%{customdata[1]:,.0f}<br>"
                "Premium vs global: %{customdata[2]:+.1f}%<br>"
                "Respondents: %{customdata[3]:,}<extra></extra>"
            ),
        )
    )

    # Global median reference line
    fig.add_vline(
        x=global_med,
        line_color="#00D4FF",
        line_width=1.5,
        line_dash="dash",
        annotation_text=f"Global median ${global_med:,.0f}",
        annotation_font_color="#00D4FF",
        annotation_font_size=11,
        annotation_position="top right",
    )

    fig.update_layout(
        **CHART_LAYOUT,
        height=max(420, len(df_chart) * 28),
        title=dict(
            text=f"Median Salary by Technology · {sel_year}"
                 "  (error bars = IQR · opacity = confidence)",
            font_size=14,
            x=0,
        ),
        showlegend=False,
    )
    fig.update_xaxes(tickprefix="$", tickformat=",", gridcolor=GRID_COLOR, title="Annual Salary (USD)")
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    # ── PREMIUM SCATTER ────────────────────────────────────────────────────────
    st.markdown('<div class="section-header">Salary Premium vs Respondent Confidence</div>', unsafe_allow_html=True)

    fig2 = px.scatter(
        df_agg,
        x="salary_premium_pct",
        y="median_salary",
        size="respondent_count",
        color="technology_category",
        hover_name="technology_name",
        labels={
            "salary_premium_pct": "Salary Premium vs Global Median (%)",
            "median_salary": "Median Salary (USD)",
            "respondent_count": "Respondents",
            "technology_category": "Category",
        },
        color_discrete_sequence=COLOR_SEQ,
        size_max=40,
    )
    fig2.add_vline(x=0, line_color="rgba(255,255,255,0.267)", line_width=1)
    fig2.update_layout(
        **CHART_LAYOUT,
        height=420,
        title=dict(
            text="Salary premium % vs median salary (bubble size = respondent count)",
            font_size=14,
            x=0,
        ),
    )
    fig2.update_xaxes(ticksuffix="%", gridcolor=GRID_COLOR, zeroline=True)
    fig2.update_yaxes(tickprefix="$", tickformat=",", gridcolor=GRID_COLOR)
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("View raw data"):
        st.dataframe(
            df_agg[
                [
                    "technology_name", "technology_category",
                    "median_salary", "p25_salary", "p75_salary",
                    "salary_premium_pct", "respondent_count",
                ]
            ].sort_values("median_salary", ascending=False),
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — DATA ROLES SKILLS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🧑‍💻  Data Roles Skills":
    st.markdown('<div class="page-title">Data Roles Skills</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="page-subtitle">'
        "Most common skills for Data Engineers, Data Scientists, Data Analysts & DB Admins · Stack Overflow Survey 2022–2025"
        "</div>",
        unsafe_allow_html=True,
    )

    # ── SIDEBAR CONTROLS ─────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")
        sel_year_dr = st.selectbox("Survey Year", [2025, 2024, 2023, 2022], key="dr_year")
        sel_role = st.selectbox("Role", list(_ROLE_ALIASES.keys()), key="dr_role")
        sel_skill_cat = st.selectbox("Skill Category", list(_SKILL_COLS.keys()), key="dr_skill")
        top_n_dr = st.slider("Top N skills", 5, 20, 12, key="dr_topn")
        compare_mode = st.checkbox("Compare all roles", key="dr_compare")

    # ── LOAD DATA ─────────────────────────────────────────────────────────────
    df_raw = load_data_roles_skills(sel_year_dr)

    if df_raw.empty:
        st.warning("No data found for the selected year.")
    else:
        role_counts = df_raw.groupby("role")["DevType"].count().rename("respondents")

        # ── METRICS ──────────────────────────────────────────────────────────
        cols_m = st.columns(len(_ROLE_ALIASES))
        for i, role in enumerate(_ROLE_ALIASES.keys()):
            n = role_counts.get(role, 0)
            cols_m[i].metric(role, f"{n:,}")

        # ── MAIN CHART ───────────────────────────────────────────────────────
        st.markdown(
            f'<div class="section-header">{sel_skill_cat} · {sel_year_dr}</div>',
            unsafe_allow_html=True,
        )

        if compare_mode:
            # Side-by-side horizontal bars for all roles
            frames = []
            for role in _ROLE_ALIASES.keys():
                subset = df_raw[df_raw["role"] == role]
                tdf = top_skills_for_role(subset, sel_skill_cat, top_n_dr)
                tdf["role"] = role
                frames.append(tdf)
            df_compare = pd.concat(frames, ignore_index=True)

            if df_compare.empty:
                st.info("No data for this skill category.")
            else:
                fig = px.bar(
                    df_compare,
                    x="pct",
                    y="skill",
                    color="role",
                    orientation="h",
                    barmode="group",
                    labels={"pct": "% of respondents", "skill": "", "role": "Role"},
                    color_discrete_sequence=COLOR_SEQ,
                    category_orders={"skill": df_compare.groupby("skill")["pct"].max().sort_values(ascending=True).index.tolist()},
                )
                fig.update_layout(
                    **CHART_LAYOUT,
                    height=max(400, len(df_compare["skill"].unique()) * 30),
                    title=dict(
                        text=f"{sel_skill_cat} — All Roles · {sel_year_dr}",
                        font_size=15,
                        x=0,
                    ),
                )
                fig.update_xaxes(ticksuffix="%", gridcolor=GRID_COLOR)
                fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig, use_container_width=True)
        else:
            # Single role horizontal bar
            subset = df_raw[df_raw["role"] == sel_role]
            tdf = top_skills_for_role(subset, sel_skill_cat, top_n_dr)

            if tdf.empty:
                st.info("No data for this combination.")
            else:
                fig = px.bar(
                    tdf,
                    x="pct",
                    y="skill",
                    orientation="h",
                    text=tdf["pct"].apply(lambda x: f"{x:.1f}%"),
                    labels={"pct": "% of respondents", "skill": ""},
                    color="pct",
                    color_continuous_scale="Blues",
                )
                fig.update_traces(textposition="outside")
                fig.update_layout(
                    **CHART_LAYOUT,
                    height=max(400, len(tdf) * 36),
                    title=dict(
                        text=f"Top {top_n_dr} {sel_skill_cat} · {sel_role} · {sel_year_dr}",
                        font_size=15,
                        x=0,
                    ),
                    showlegend=False,
                    coloraxis_showscale=False,
                )
                fig.update_xaxes(ticksuffix="%", gridcolor=GRID_COLOR, range=[0, tdf["pct"].max() * 1.2])
                fig.update_yaxes(gridcolor="rgba(0,0,0,0)", categoryorder="total ascending")
                st.plotly_chart(fig, use_container_width=True)

        # ── TREND OVER YEARS ─────────────────────────────────────────────────
        st.markdown('<div class="section-header">Skill Trend 2022–2025</div>', unsafe_allow_html=True)
        st.caption("How the top skills for the selected role have changed year over year.")

        trend_frames = []
        for yr in [2022, 2023, 2024, 2025]:
            try:
                df_yr = load_data_roles_skills(yr)
                subset = df_yr[df_yr["role"] == sel_role]
                tdf = top_skills_for_role(subset, sel_skill_cat, 10)
                tdf["year"] = yr
                trend_frames.append(tdf)
            except Exception:
                pass

        if trend_frames:
            df_trend = pd.concat(trend_frames, ignore_index=True)
            # Keep only skills that appear in current year's top list
            current_top = top_skills_for_role(
                df_raw[df_raw["role"] == sel_role], sel_skill_cat, 10
            )["skill"].tolist()
            df_trend = df_trend[df_trend["skill"].isin(current_top)]

            if not df_trend.empty:
                fig_trend = px.line(
                    df_trend,
                    x="year",
                    y="pct",
                    color="skill",
                    markers=True,
                    labels={"pct": "% of respondents", "year": "Year", "skill": "Skill"},
                    color_discrete_sequence=COLOR_SEQ,
                )
                fig_trend.update_traces(line_width=2, marker_size=6)
                fig_trend.update_layout(
                    **CHART_LAYOUT,
                    height=420,
                    title=dict(
                        text=f"{sel_skill_cat} Trend · {sel_role}",
                        font_size=15,
                        x=0,
                    ),
                    hovermode="x unified",
                )
                fig_trend.update_xaxes(tickmode="linear", dtick=1, gridcolor=GRID_COLOR)
                fig_trend.update_yaxes(ticksuffix="%", gridcolor=GRID_COLOR)
                st.plotly_chart(fig_trend, use_container_width=True)

        # ── AI INSIGHT ────────────────────────────────────────────────────────
        st.markdown('<div class="section-header">🤖 AI Insight</div>', unsafe_allow_html=True)

        subset_cur = df_raw[df_raw["role"] == sel_role]
        tdf_ai = top_skills_for_role(subset_cur, sel_skill_cat, 10)
        skill_summary = ", ".join(
            f"{row['skill']} ({row['pct']}%)" for _, row in tdf_ai.iterrows()
        )
        n_respondents = role_counts.get(sel_role, 0)

        ai_context = (
            f"Role: {sel_role}\n"
            f"Year: {sel_year_dr}\n"
            f"Respondents: {n_respondents}\n"
            f"Skill category: {sel_skill_cat}\n"
            f"Top skills (% of respondents who used it): {skill_summary}"
        )

        if st.button("✨ Generate AI Insight", key="btn_insight_dr"):
            with st.spinner("Asking AI…"):
                insight = generate_insight(
                    context=ai_context,
                    prompt_prefix=(
                        "You are a data industry career analyst. "
                        "Write exactly 3 concise, data-grounded sentences about the skill landscape "
                        "for this data role based on the survey data below. "
                        "Focus on what skills dominate, any surprising inclusions, and what this means "
                        "for someone entering or growing in this role. Be direct — no preamble.\n\n"
                        "Data snapshot:\n"
                    ),
                )
            st.markdown(
                f"""
                <div class="insight-box">
                  <div class="label">AI Narrative · Claude</div>
                  <p>{insight}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # ── RAW DATA ─────────────────────────────────────────────────────────
        with st.expander("View raw skill counts"):
            if not compare_mode:
                subset_raw = df_raw[df_raw["role"] == sel_role]
                st.dataframe(
                    top_skills_for_role(subset_raw, sel_skill_cat, 30),
                    use_container_width=True,
                )
