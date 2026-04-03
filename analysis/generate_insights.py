"""
Tech Ecosystem Tracker - AI-Powered Insight Generation
Uses Anthropic Claude API to analyze mart data and generate narrative insights.
"""

import os
import json
import anthropic
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="TECH_ECOSYSTEM",
        schema="ANALYTICS",
        warehouse="TECH_WH",
    )


def query_to_dict(conn, sql: str) -> list[dict]:
    """Run SQL and return results as list of dicts."""
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def fetch_analysis_data(conn) -> dict:
    """Pull key datasets from mart tables for Claude to analyze."""

    said_vs_did = query_to_dict(conn, """
        SELECT year, technology_name, technology_category,
               avg_survey_adoption_pct, github_activity_pct,
               said_vs_did_gap, said_vs_did_classification
        FROM fct_said_vs_did
        WHERE year >= 2022
        ORDER BY abs(said_vs_did_gap) DESC
        LIMIT 30
    """)

    sentiment = query_to_dict(conn, """
        SELECT year, technology_name, technology_category,
               adoption_pct, desire_pct, desire_gap,
               adoption_yoy_change, lifecycle_stage
        FROM fct_technology_sentiment
        WHERE year = (SELECT MAX(year) FROM fct_technology_sentiment)
        ORDER BY desire_gap DESC
        LIMIT 25
    """)

    salary = query_to_dict(conn, """
        SELECT technology_name, technology_category,
               AVG(median_salary) as avg_median_salary,
               AVG(salary_premium_pct) as avg_premium_pct,
               SUM(respondent_count) as total_respondents
        FROM fct_salary_by_stack
        WHERE survey_year = (SELECT MAX(survey_year) FROM fct_salary_by_stack)
          AND country = 'United States'
        GROUP BY 1, 2
        HAVING total_respondents >= 50
        ORDER BY avg_premium_pct DESC
        LIMIT 20
    """)

    emerging = query_to_dict(conn, """
        SELECT technology_name, lifecycle_stage,
               adoption_pct, desire_pct, desire_gap
        FROM fct_technology_sentiment
        WHERE lifecycle_stage IN ('emerging', 'declining')
          AND year = (SELECT MAX(year) FROM fct_technology_sentiment)
        ORDER BY lifecycle_stage, desire_gap DESC
    """)

    return {
        "said_vs_did": said_vs_did,
        "sentiment": sentiment,
        "salary_premiums": salary,
        "emerging_and_declining": emerging,
    }


def generate_insights(data: dict) -> str:
    """Send data to Claude and get narrative analysis."""
    client = anthropic.Anthropic()

    prompt = f"""You are a senior technology analyst. Analyze the following data from the
Tech Ecosystem Tracker, which combines Stack Overflow Developer Survey data,
JetBrains Developer Ecosystem Survey data, and GitHub Archive activity data.

Generate a concise, insight-rich report covering these three areas:

## 1. Said vs. Did Analysis
Compare what developers SAY they use (survey data) with what they ACTUALLY do (GitHub activity).
Data: {json.dumps(data['said_vs_did'], indent=2, default=str)}

## 2. Emerging vs. Dying Technologies
Identify which technologies are gaining momentum and which are losing relevance.
Data: {json.dumps(data['emerging_and_declining'], indent=2, default=str)}
Sentiment data: {json.dumps(data['sentiment'], indent=2, default=str)}

## 3. Salary Premiums by Stack
Which tech stacks command the highest salary premiums?
Data: {json.dumps(data['salary_premiums'], indent=2, default=str)}

Format your response as a professional report with clear sections, key findings highlighted,
and actionable insights for developers and engineering leaders. Include specific numbers
from the data to support your claims. Keep it under 1500 words."""

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text


def main():
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()

    print("Fetching analysis data from mart tables...")
    data = fetch_analysis_data(conn)

    print(f"Data summary:")
    for key, rows in data.items():
        print(f"  {key}: {len(rows)} rows")

    print("\nGenerating insights with Claude...")
    report = generate_insights(data)

    # Save report
    output_path = os.path.join(os.path.dirname(__file__), "tech_ecosystem_report.md")
    with open(output_path, "w") as f:
        f.write(f"# Tech Ecosystem Tracker Report\n\n")
        f.write(f"_Generated on {pd.Timestamp.now().strftime('%Y-%m-%d')}_\n\n")
        f.write(report)

    print(f"\nReport saved to: {output_path}")
    print("\n" + "=" * 60)
    print(report)

    conn.close()


if __name__ == "__main__":
    main()
