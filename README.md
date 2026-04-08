# 🔍 Tech Ecosystem Tracker

> **Quantifying the gap between what developers *say* they use and what they *actually* use**

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.5+-orange.svg)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DW-29B5E8.svg)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B.svg)](https://streamlit.io/)
[![Claude API](https://img.shields.io/badge/Claude-API-191919.svg)](https://www.anthropic.com/)

---

## 🎯 Problem Statement

Developer surveys paint one picture of the tech landscape, but GitHub activity tells a different story. This project bridges that gap by:

- **Comparing survey sentiment vs. actual usage** - Are developers overstating trendy frameworks?
- **Tracking salary premiums by stack** - Which technologies command higher compensation?
- **Identifying emerging vs. dying technologies** - What's gaining traction vs. fading away?
- **Detecting adoption anomalies** - Unusual spikes or drops in usage patterns

**Key Insight:** Developers over-report "hot" technologies (e.g., Rust, Go) by ~15-20% compared to actual GitHub usage, while under-reporting "boring" but widely-used tools (e.g., jQuery, PHP).

---

## 🏗️ Architecture

![Architecture Diagram](architecture_diagram.svg)

**Data Flow:**
1. Raw CSVs ingested into Snowflake `RAW` schema via Python ingestion scripts
2. dbt staging models clean and standardize each source independently
3. Intermediate layer creates a unified technology mapping (`int_unified__technology_spine`) to reconcile naming across sources
4. Marts layer produces fact tables (`fct_said_vs_did`, `fct_technology_adoption`, `fct_salary_by_stack`, `fct_technology_sentiment`) and `dim_technologies`
5. Streamlit dashboard queries marts; Claude API generates narrative insights on demand
6. Monitoring layer (`alert_adoption_anomalies`) flags unusual adoption patterns

---

## 📊 Data Sources

| Source | Time Period | Records | Key Metrics |
|--------|-------------|---------|-------------|
| **Stack Overflow Developer Survey** | 2021–2025 | ~500K total | Language/framework usage, salaries, satisfaction |
| **JetBrains Developer Ecosystem** | 2023–2025 | ~100K total | Tool preferences, primary languages, team sizes |
| **GitHub Archive (BigQuery)** | Aggregated | Millions of repos | Actual language/framework usage in commits |

**Data Quality:**
- Survey data cleaned for inconsistent naming (e.g., "JavaScript" vs "Javascript" vs "JS")
- GitHub data aggregated to prevent skew from bot/generated code repos
- Cross-source technology mapping via `int_unified__technology_spine` model

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Warehouse** | Snowflake | Cloud DW (TECH_ECOSYSTEM db, TECH_WH warehouse) |
| **Transformation** | dbt | Version-controlled SQL transformations, testing, docs |
| **Modeling Approach** | Kimball Dimensional | Fact tables (adoption, salary, sentiment) + dimension (technologies) |
| **Orchestration** | Snowflake Tasks | Scheduled dbt runs (can migrate to Airflow/Prefect) |
| **Visualization** | Streamlit in Snowflake | Interactive dashboard deployed inside Snowflake |
| **AI Integration** | Anthropic Claude API | Automated insight generation from trend data |
| **Monitoring** | Custom dbt model | Anomaly detection using statistical thresholds |
| **Languages** | Python, SQL | Data profiling scripts, dbt models |

---

## 🗂️ Project Structure

```
tech-ecosystem-tracker/
│
├── models/                          # dbt models (Kimball dimensional design)
│   ├── staging/                     # Source-specific cleaning
│   │   ├── stackoverflow/
│   │   │   ├── stg_stackoverflow__surveys.sql
│   │   │   └── stg_stackoverflow__technologies_unpivoted.sql
│   │   ├── jetbrains/
│   │   │   ├── stg_jetbrains__surveys.sql
│   │   │   └── stg_jetbrains__technologies_unpivoted.sql
│   │   └── github/
│   │       └── stg_github__monthly_activity.sql
│   │
│   ├── intermediate/                # Cross-source integration
│   │   ├── int_unified__technology_spine.sql    # Critical cross-source mapping layer
│   │   ├── int_stackoverflow__yearly_adoption.sql
│   │   ├── int_jetbrains__yearly_adoption.sql
│   │   └── int_github__yearly_activity.sql
│   │
│   ├── marts/                       # Business logic layer
│   │   ├── fct_said_vs_did.sql              # Survey vs. GitHub delta
│   │   ├── fct_technology_adoption.sql       # Adoption trends over time
│   │   ├── fct_salary_by_stack.sql           # Compensation analysis
│   │   ├── fct_technology_sentiment.sql      # Developer satisfaction
│   │   └── dim_technologies.sql              # Technology dimension
│   │
│   └── monitoring/                  # Data quality & alerts
│       └── alert_adoption_anomalies.sql
│
├── app/
│   └── streamlit_app.py            # Streamlit dashboard with Claude API integration
│
├── scripts/
│   ├── data_profiling.py           # Data quality profiling + output reports
│   ├── load_stackoverflow.py       # Stack Overflow CSV → Snowflake
│   ├── load_jetbrains.py           # JetBrains CSV → Snowflake
│   ├── load_jetbrains_2021_2022.py # Historical JetBrains backfill
│   ├── load_github_api.py          # GitHub data → Snowflake
│   ├── deploy_streamlit.py         # Deploy app to Streamlit in Snowflake
│   └── rollback_streamlit.py       # Rollback Streamlit deployment
│
├── snowflake/                      # Snowflake infrastructure SQL
│   ├── 01_setup_infrastructure.sql
│   ├── 02_raw_tables.sql
│   ├── 03_stages_and_loading.sql
│   └── 04_streamlit_setup.sql
│
├── monitoring/                     # Operational monitoring queries
│   ├── data_freshness_check.sql
│   └── quality_dashboard.sql
│
├── profiling_output/               # Auto-generated data profiling charts
├── analysis/                       # Ad-hoc analysis notebooks
├── docs/                           # Project documentation
│
├── dbt_project.yml                 # dbt configuration
├── profiles.yml                    # Snowflake connection config
├── packages.yml                    # dbt packages (dbt-utils, etc.)
└── requirements.txt                # Python dependencies
```

---

## 🚀 Key Features

### 1. Said vs. Did Analysis
Quantifies the gap between survey responses and actual GitHub usage:
- **Example Finding:** React reported by 68% of survey respondents but appears in only 42% of analyzed repos (26% overstatement)
- **Insight:** Trendy frameworks get over-reported; established tools get under-reported

### 2. Salary Intelligence
Correlates technology stacks with reported compensation:
- **Top Premium:** Rust developers earn ~18% more than average
- **Data Source:** Stack Overflow salary data cross-referenced with primary language usage

### 3. Trend Detection
Statistical analysis of adoption trajectories:
- **Rising:** TypeScript (+22% YoY), Go (+15% YoY)
- **Declining:** jQuery (-18% YoY), Angular (-12% YoY)
- **Methodology:** Year-over-year comparison with confidence intervals

### 4. AI-Powered Insights
Claude API integration generates narrative analysis:
- Automatically summarizes trend patterns
- Identifies correlations (e.g., "Rust growth correlates with systems programming resurgence")
- Generates executive summaries from raw metrics

### 5. Anomaly Monitoring
Statistical alerts on unusual patterns:
- Detects sudden spikes/drops in adoption (±2 standard deviations)
- Flags data quality issues (e.g., missing survey years, incomplete GitHub data)
- `models/monitoring/alert_adoption_anomalies.sql`

---

## 📈 Sample Insights

### Said vs. Did Gap (Top 10)
```
Technology    | Survey % | GitHub % | Delta
--------------|----------|----------|-------
React         | 68%      | 42%      | +26%
Rust          | 12%      | 3%       | +9%
TypeScript    | 45%      | 38%      | +7%
jQuery        | 15%      | 31%      | -16%
PHP           | 22%      | 35%      | -13%
```

### Claude-Generated Insight Example
> "TypeScript adoption has accelerated 22% year-over-year, primarily driven by frontend frameworks (React, Vue) migrating from JavaScript. This correlates with a 14% salary premium for TypeScript-proficient developers. The trend suggests continued growth as type safety becomes table stakes for large-scale applications."

---

## 💻 Setup & Installation

### Prerequisites
- Python 3.9+
- Snowflake account with `ACCOUNTADMIN` or database creation privileges
- Anthropic API key (for Claude integration)

### 1. Clone Repository
```bash
git clone https://github.com/amateeraasu/tech-ecosystem-tracker.git
cd tech-ecosystem-tracker
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Snowflake Connection
Update `profiles.yml` with your credentials:
```yaml
tech_ecosystem_tracker:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT
      user: YOUR_USER
      password: YOUR_PASSWORD
      role: YOUR_ROLE
      database: TECH_ECOSYSTEM
      warehouse: TECH_WH
      schema: RAW_ANALYTICS
      threads: 4
```

### 4. Set Up Environment Variables
```bash
cp .env.example .env
# Edit .env and add:
# ANTHROPIC_API_KEY=your_api_key_here
# SNOWFLAKE_ACCOUNT=your_account
```

### 5. Initialize Snowflake Infrastructure
```bash
# Run scripts in order in a Snowflake worksheet (or via SnowSQL):
snowflake/01_setup_infrastructure.sql
snowflake/02_raw_tables.sql
snowflake/03_stages_and_loading.sql
```

### 6. Load Raw Data
```bash
python scripts/load_stackoverflow.py
python scripts/load_jetbrains.py
python scripts/load_github_api.py
```

### 7. Run dbt Transformations
```bash
dbt deps          # Install dbt packages
dbt run           # Execute all models
dbt test          # Run data quality tests
dbt docs generate # Generate documentation
dbt docs serve    # View lineage + docs locally
```

### 8. Launch Streamlit Dashboard
```bash
# Local development
streamlit run app/streamlit_app.py
# Dashboard available at http://localhost:8501

# Deploy to Streamlit in Snowflake
python scripts/deploy_streamlit.py
```

---

## 🔍 For Recruiters & Hiring Managers

### What This Project Demonstrates

**Analytics Engineering:**
- ✅ Multi-source data integration (3 different survey formats + BigQuery aggregations)
- ✅ Dimensional modeling (Kimball methodology — fact/dimension tables)
- ✅ dbt best practices (staging → intermediate → marts, schema.yml tests, documentation)
- ✅ Data quality & monitoring (anomaly detection, profiling, dbt tests)
- ✅ Version-controlled SQL transformations

**Data Engineering:**
- ✅ Cloud data warehouse setup (Snowflake multi-schema architecture)
- ✅ ELT pipelines (Python ingestion → Snowflake → dbt)
- ✅ Incremental loading strategies (time-based partitioning)
- ✅ Data profiling & validation (`scripts/data_profiling.py`)

**Technical Breadth:**
- ✅ SQL (window functions, CTEs, cross-source joins)
- ✅ Python (pandas, Snowflake connector, API integration)
- ✅ Modern data stack (Snowflake + dbt + Streamlit)
- ✅ AI/LLM integration (production Claude API usage with structured prompts)

---

## 📚 Key Technical Challenges

**1. Cross-Source Technology Mapping**
- **Challenge:** Each survey uses different naming conventions ("JavaScript" vs "JS" vs "ECMAScript")
- **Solution:** `int_unified__technology_spine.sql` — a canonical mapping layer that all downstream models join against
- **Learning:** Semantic/mapping layers are essential in multi-source analytics; without them, every mart becomes a naming-normalization exercise

**2. Survey Bias Quantification**
- **Challenge:** Fairly comparing self-reported survey data with GitHub activity (different populations, different sampling)
- **Solution:** Normalized both datasets to percentages; controlled for repo popularity skew in GitHub aggregation
- **Learning:** Need to account for systematic bias (GitHub skews toward open-source; surveys skew toward employed developers)

**3. Incremental dbt Models**
- **Challenge:** Full refresh across 5 years of survey data is slow and expensive
- **Solution:** Incremental models using `is_incremental()` logic + Snowflake clustering keys
- **Learning:** Snowflake clustering keys on `survey_year` significantly reduce scan costs on large fact tables

**4. Claude API for Dynamic Insights**
- **Challenge:** Manually writing trend analysis for every permutation is not scalable
- **Solution:** Structured prompts to Claude API with aggregated trend data → narrative output embedded in the dashboard
- **Learning:** LLMs excel at pattern summarization given well-structured input data; prompt engineering matters as much as data quality

---

## 🔮 Future Enhancements

- [ ] Real-time GitHub ingestion via GitHub API (vs. static BigQuery exports)
- [ ] Airflow/Prefect orchestration for scheduled dbt runs + alerting
- [ ] Job posting data integration (Indeed/LinkedIn) for demand-side signals
- [ ] Time-series forecasting (Prophet/ARIMA) for adoption trend predictions
- [ ] dbt exposures linking dashboard views to specific source models
- [ ] Great Expectations for advanced data contract validation

---

## 📝 Known Limitations

- **GitHub data is aggregated** — no raw commit-level analysis (BigQuery costs)
- **Survey data requires manual download** — Stack Overflow/JetBrains don't provide public APIs
- **Claude API costs** — ~$0.02–0.05 per insight generation (cached prompts help)
- **Snowflake costs** — recommend X-Small warehouse for dev/testing

### Data Freshness
- Survey data updated annually (Stack Overflow: April, JetBrains: Q1)
- GitHub data: static export (last updated Q1 2025)
- Refresh cadence: quarterly for full pipeline

---

## 🤝 Contact

**Azhar Kudaibergenova** — Analytics Engineer  
[LinkedIn](https://www.linkedin.com/in/azhar-kudaibergenova) | [GitHub](https://github.com/amateeraasu)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

*Built with SQL, Python, and too many browser tabs open.*
