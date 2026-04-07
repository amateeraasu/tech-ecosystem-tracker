# Tech Ecosystem Tracker Report

_Generated on 2026-04-03_

# Tech Ecosystem Tracker: Developer Trends Report
### Synthesizing Stack Overflow, JetBrains & GitHub Archive Data

---

## Executive Summary

Developer self-reporting systematically overstates tool usage, aspirational technologies are diverging sharply from incumbent stacks, and the highest-paying languages command salary premiums exceeding 100% above baseline. Engineering leaders who calibrate decisions against actual GitHub activity — not survey sentiment — will gain a significant competitive edge.

---

## Section 1: Said vs. Did — The Perception Gap

### Key Finding: Developers Consistently Overstate Their Tool Usage

Every technology with matched survey and GitHub data shows a positive gap — meaning developers *claim* higher usage than their commits reflect. This isn't noise; it's a structural pattern.

**The Largest Gaps (Survey % minus GitHub Activity %):**

| Technology | Survey Adoption | GitHub Activity | Gap |
|---|---|---|---|
| JavaScript | 63.4% (2023) | 31.9% | **+31.5 pts** |
| Python | 50.9% (2023) | 16.6% | **+34.3 pts** |
| Python | 51.0% (2024) | 19.8% | **+31.2 pts** |
| JavaScript | 59.7% (2024) | 28.7% | **+31.0 pts** |

Python and JavaScript consistently show ~30-point inflation across multiple years — not a survey artifact, but a durable signal.

**Technologies with No GitHub Footprint:**
SQL, HTML/CSS, PostgreSQL, MySQL, React, Node.js, AWS, and Docker all appear in the "over_reported" category with *zero measurable GitHub activity* in this dataset. The gaps are mechanically equal to their survey adoption rates (37–53 points for SQL and HTML/CSS). This likely reflects a **measurement boundary**: these technologies live in private repositories, cloud consoles, or non-code artifacts that GitHub Archive doesn't capture well.

**What's Actually Consistent:**
Shell scripting holds steady at ~1.9–2.2% GitHub activity across 2023–2025 with no survey counterpart — suggesting it's an honest, unglamorous workhorse that developers don't bother claiming in surveys but quietly depend on every day.

### Actionable Insights
- **For hiring managers:** Treat self-reported proficiency claims skeptically. A candidate claiming Python expertise may have primarily used it as a secondary tool. Require portfolio evidence or code assessments.
- **For survey consumers:** Normalize survey data against a ~30-point inflation factor for dominant languages when estimating true active usage.
- **For platform teams:** Shell scripting's quiet consistency signals it remains critical infrastructure glue — don't neglect tooling investment here.

---

## Section 2: Emerging vs. Dying Technologies

### Key Finding: Desire is Collapsing Faster Than Adoption — Even for Dominant Tools

The most striking pattern in this data is not that niche tools are declining — it's that the *entire incumbent stack* shows negative desire gaps. Developers are actively using technologies they no longer *want* to use.

**The Declining Majority — Notable Cases:**

| Technology | Adoption % | Desire % | Gap |
|---|---|---|---|
| JavaScript | 42.7% | 21.5% | **-21.2 pts** |
| HTML/CSS | 40.0% | 21.7% | **-18.4 pts** |
| SQL | 37.9% | 22.9% | **-15.0 pts** |
| Python | 37.4% | 25.3% | **-12.2 pts** |
| MySQL | 21.5% | 10.4% | **-11.1 pts** |
| Node.js | 23.5% | 13.8% | **-9.6 pts** |
| React | 21.5% | 14.3% | **-7.3 pts** |

JavaScript's -21.2 point desire gap is the widest in the dataset. Nearly half of all developers use it; barely half of those *want* to. This is the clearest indicator of **technological debt at ecosystem scale**.

**Legacy Technologies Accelerating Out:**
PHP (-6.4 pts, declining YoY), jQuery (-7.2 pts), and ASP.NET (-3.9 pts) are losing desire at rates proportional to their aging user bases. These are managed declines, not surprises.

**Surprising Declines:**
- **FastAPI** (-1.18 pts desire gap, -0.22% YoY adoption): Once the Python framework darling, it's plateauing quickly.
- **Dart/Flutter** (-0.47 pts, -1.66% YoY adoption): Flutter's mobile promise hasn't translated into sustained developer enthusiasm.
- **Next.js** (-3.06 pts): Despite strong mindshare, desire is eroding — possibly due to framework complexity and frequent breaking changes.

**The Emerging and Growing-Interest Cohort:**

Only **Deno** qualifies as truly *emerging* (+1.08 pts desire gap, +0.51% YoY adoption growth) — making it the lone technology in the dataset gaining both desire *and* usage simultaneously.

The **growing interest** cohort is more nuanced — high desire but declining adoption, suggesting aspirational friction:

| Technology | Adoption % | Desire % | Gap | YoY Adoption |
|---|---|---|---|---|
| Rust | 9.6% | 18.8% | **+9.2 pts** | -1.95% |
| Go | 10.6% | 15.1% | **+4.5 pts** | -1.77% |
| Zig | 1.4% | 4.9% | **+3.5 pts** | +0.36% |
| Elixir | 1.7% | 3.7% | **+1.98 pts** | -0.18% |
| Svelte | 3.5% | 5.2% | **+1.68 pts** | -1.32% |

Rust's +9.2 desire gap is the highest in the dataset — but its adoption *fell* 1.95 points YoY. Developers want to use Rust; the learning curve and ecosystem maturity are slowing them down. Go shows a similar pattern. **Zig is the exception**: a modest but real +0.36% adoption growth alongside strong desire suggests early-stage genuine uptake.

**Bun** (2.73% adoption, +0.31 desire gap) enters the dataset without prior YoY data but shows immediate positive desire — worth watching as a Node.js challenger.

### Actionable Insights
- **For developers:** Rust, Go, and Zig offer the best combination of high desire and salary premium (see Section 3). The investment cost is real but quantifiably worth it.
- **For engineering leaders:** The negative desire gap for JavaScript and SQL doesn't mean abandoning them — it means prioritizing developer experience investments (better tooling, TypeScript migration, ORM abstraction) to reduce the friction of mandatory tools.
- **For framework decisions:** Avoid locking into Next.js or FastAPI as long-term strategic bets without evaluating emerging alternatives. Svelte and Deno show healthier desire trajectories at smaller scale.

---

## Section 3: Salary Premiums — Where the Money Is

### Key Finding: Functional and Systems Languages Command Extraordinary Premiums

Salary premiums are calculated against a baseline, and the top of the table is dominated by languages that fewer than 5% of developers use — but that pay over 100% above that baseline.

**Top Salary Premium Technologies:**

| Technology | Avg. Median Salary | Premium vs. Baseline | Respondents |
|---|---|---|---|
| Scala | $203,633 | **+103.6%** | 267 |
| Clojure | $196,102 | **+96.1%** | 201 |
| Cassandra | $193,289 | **+93.3%** | 184 |
| Go | $190,488 | **+90.5%** | 1,176 |
| DynamoDB | $189,278 | **+89.3%** | 978 |
| Elixir | $188,969 | **+89.0%** | 272 |
| Rust | $178,663 | **+78.7%** | 824 |
| Kotlin | $177,365 | **+77.4%** | 598 |

**The Scale-Adjusted Winners:**
Small respondent pools can inflate premiums (