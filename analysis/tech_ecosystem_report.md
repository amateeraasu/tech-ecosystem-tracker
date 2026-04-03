# Tech Ecosystem Tracker Report

_Generated on 2026-04-03_

# Tech Ecosystem Tracker: Developer Trends Report 2025

---

## Executive Summary

This report synthesizes survey self-reporting, GitHub activity data, and compensation signals to surface the gaps between developer perception and practice, identify which technologies are gaining or losing momentum, and highlight where the highest salary premiums concentrate. The findings reveal a widening credibility gap in survey data, a broad "desire fatigue" across established tools, and a clear compensation premium clustering around systems languages and specialized data infrastructure.

---

## Section 1: Said vs. Did — The Self-Reporting Gap

### Key Finding: Developers Systematically Overstate Their Use of Popular Tools

The most consistent pattern across 2022–2025 is that developers significantly over-report usage of mainstream technologies in surveys relative to their actual GitHub commit activity. This is not a minor rounding error — it is a structural distortion.

**The largest absolute gaps (survey % minus GitHub activity %):**

| Technology | Survey Adoption | GitHub Activity | Gap |
|---|---|---|---|
| JavaScript | 63.4% (2023) | 31.9% | **+31.5 pp** |
| Python | 51.0% (2023) | 16.6% | **+34.3 pp** |
| HTML/CSS | 53.4% (2022) | No GitHub signal | **+53.4 pp** |
| SQL | 52.5% (2023) | No GitHub signal | **+52.5 pp** |

**What this means:** JavaScript and Python respondents claim adoption at roughly **2× their actual GitHub footprint**. Technologies like SQL, HTML/CSS, PostgreSQL, and MySQL show *no measurable GitHub repository activity* in this dataset — likely because they are embedded in projects rather than surfacing as standalone file activity — yet they dominate survey responses at 38–53%.

**The "Tool Identity" Effect:** Developers appear to claim proficiency with ubiquitous tools (SQL, HTML/CSS) as professional identity markers rather than as precise usage signals. A developer who writes one SQL query per month may still legitimately check "SQL user" on a survey.

**Shell is the honest outlier.** Shell scripting shows ~2.0–2.2% GitHub activity with no survey counterpart — it is *used* without being *claimed*, the inverse of the over-reporting pattern. This suggests Shell is treated as infrastructure rather than a career skill.

**Actionable Insight for Leaders:** Treat survey-based technology adoption numbers as *upper bounds*, not ground truth. For workforce planning or vendor decisions, triangulate against repository activity, CI/CD tooling logs, and package manager data. Python's real footprint is roughly half what surveys suggest; JavaScript's is closer to 30% of public repositories, not 60%+.

---

## Section 2: Emerging vs. Dying Technologies

### Key Finding: Desire Has Broadly Collapsed — Almost Nothing Is Gaining Ground

The lifecycle data presents a striking and somewhat alarming picture: **43 out of 44 technologies in the dataset are classified as "declining" or "established but waning."** Only Deno registers as unambiguously "emerging" with a positive desire gap of +1.08 pp.

### The Emerging & Growing-Interest Tier

| Technology | Adoption | Desire Gap | YoY Change | Signal |
|---|---|---|---|---|
| **Rust** | 9.6% | **+9.23 pp** | -1.95% | Desire far outpaces adoption |
| **Go** | 10.6% | **+4.46 pp** | -1.77% | Strong aspiration, adoption plateau |
| **Zig** | 1.4% | **+3.53 pp** | +0.36% | Only language growing in both desire AND adoption |
| **Elixir** | 1.7% | **+1.98 pp** | -0.18% | Niche but sticky desire premium |
| **Svelte** | 3.5% | **+1.68 pp** | -1.32% | Enthusiasm persists despite adoption dip |
| **Deno** | 1.95% | **+1.08 pp** | +0.51% | Sole "emerging" classification; growing |

**Rust** remains the most aspirational language in the dataset by a wide margin — developers *want* to use it at nearly double the rate they currently do. Yet adoption is actually *declining* year-over-year (-1.95%), suggesting a persistent gap between enthusiasm and organizational uptake. This is the "Rust paradox": universally admired, structurally under-deployed.

**Zig** is the genuine dark horse. It is the only language showing *positive* adoption growth (+0.36% YoY) alongside strong desire (+3.53 pp). With existing GitHub activity dating to 2022 and a lean systems-programming profile, Zig is the most credible niche challenger to watch.

**Deno** shows real momentum (+0.51% YoY adoption growth) and is the only platform-tier technology with a positive desire gap. Its rivalry with Node.js is not yet a threat at 1.95% adoption, but directional signals are real.

### The Declining Tier — Notable Casualties

The decline signals are broad, but severity varies sharply:

- **JavaScript** (-21.2 pp desire gap) and **HTML/CSS** (-18.4 pp) lead all technologies in desire collapse — developers are burned out on the front-end stack.
- **SQL** (-15.0 pp) and **Python** (-12.2 pp) show massive desire deficits despite still being the most-used languages — saturation, not failure.
- **MySQL** (-11.1 pp), **Node.js** (-9.6 pp), and **Java** (-8.9 pp) are all shedding enthusiasm faster than adoption.
- **FastAPI** (-1.18 pp) is a notable surprise — it rose quickly as a Python API framework darling but is already showing desire decay at only 7.1% adoption.
- **Dart/Flutter** (-1.66% YoY, -0.47 pp desire gap) is losing ground, raising questions about Flutter's long-term community trajectory outside mobile-first markets.

**The "Established But Waning" Category** (Kubernetes, Snowflake, Perl, Lua) deserves attention: these technologies have stabilized adoption but are not inspiring future demand. Kubernetes at 14.2% adoption with a -0.45 pp desire gap signals it is becoming *expected infrastructure* rather than a differentiating skill.

**Actionable Insight for Developers:** If you are early-career, investing in Rust and Go remains high-signal even if organizational adoption lags — demand will clear. Avoid doubling down on FastAPI, Flask, or Django as primary identity skills; Python web frameworks are fragmenting and losing mindshare. For front-end specialists, the data suggests growing developer fatigue with the JavaScript ecosystem broadly.

---

## Section 3: Salary Premiums by Stack

### Key Finding: Systems Languages and Specialized Data Infrastructure Command Extraordinary Premiums

The compensation data — drawn from respondents reporting salaries — reveals a consistent clustering of premium pay around three profiles: **functional/systems programming languages**, **specialized NoSQL and cloud-native databases**, and **cloud platform expertise**.

### Top 10 Salary Premiums (vs. baseline)

| Technology | Avg. Median Salary | Premium vs. Baseline | Sample Size |
|---|---|---|---|
| **Scala** | $203,633 | **+103.6%** | 267 |
| **Clojure** | $196,102 | **+96.1%** | 201 |
| **Cassandra** | $193,289 | **+93.3%** | 184 |
| **Go** | $190,488 | **+90.5%** | 1,176 |
| **DynamoDB** | $189,278 | **+89.3%** | 978 |
| **Elixir** | $188,969 | **+89.0%** | 272 |
| **Objective-C** | $185,807 | **+85.8%** | 241 |
| **Assembly** | $182,978 | **+83.0%** | 263 |
| **Ruby** | $182,751 | **+82.8%** | 851 |
| **Neo4j** | $180,259 | **+80.3%** | 155 |

**Three structural observations:**

1. **The functional language premium is real and large.** Scala (+103.6%), Clojure (+96.1%), and Elixir (+89.0%) are used by senior engineers solving