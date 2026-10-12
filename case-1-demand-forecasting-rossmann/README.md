# Demand Forecasting & Store Performance Analysis — Rossmann Store Sales

## Context

I work in demand planning, moving from forecasting consumer demand to analyzing direct sales and reseller performance. This project is the first case study in my data analytics portfolio, built to demonstrate how I approach a real business problem end-to-end: from raw, messy data to a decision-ready analytics layer.

I used the [Rossmann Store Sales dataset](https://www.kaggle.com/c/rossmann-store-sales) (Kaggle), which contains ~3 years of daily sales data for 1,115 drug stores across Germany, including promotions, holidays, competition, and store metadata.

## Problem

A retail chain needs to understand how its stores are performing, spot abnormal sales variations early, and support planning decisions — without waiting for a human analyst to manually dig through raw data every time. The raw dataset also has real-world data quality issues (nulls, inconsistent types, categorical fields that look numeric), which have to be resolved before any number can be trusted.

Specific business questions this project answers:
- Which stores are top/bottom performers, and by how much?
- How is revenue trending over time, and is there a reliable way to flag weeks where sales dropped sharply versus the recent trend?
- Does a public holiday, Easter, or Christmas meaningfully change sales behavior?

## Technical Approach

I used **Google BigQuery** (Sandbox mode — free tier, no billing required) as the environment, with a layered SQL pipeline:

1. **Raw layer**: CSV files loaded as-is, all columns as `STRING`. This is intentional — it lets me validate data quality before committing to types, instead of trusting the source blindly.
2. **Data quality checks**: used `SAFE_CAST` and `COUNTIF` to measure, column by column, how many values fail to cast cleanly (nulls, malformed dates, unexpected categories). This is also where I identified that `StateHoliday` is a categorical flag (`0`/`a`/`b`/`c` = none/public holiday/Easter/Christmas), confirmed against Kaggle's official data dictionary, and that the `Customers` field is a **daily count**, not a customer ID — both findings directly shaped how the metrics below are calculated.
3. **Staging layer**: typed, validated columns built with `SAFE_CAST` / `SAFE.PARSE_DATE`, so downstream tables only ever read clean data.
4. **Partitioned & clustered table** (`sales_partitioned`): partitioned by date and clustered by store, to keep the pipeline efficient and demonstrate production-style table design, not just ad-hoc querying.
5. **Trend layer** (`sales_with_trends`): built with window functions (`AVG() OVER`, `LAG() OVER`) to calculate rolling averages and week-over-week variation per store, without distorting results with simple row-level averages.
6. **Aggregated layers**:
   - `monthly_summary`: revenue, customer count, and average ticket by month — refreshed through a stored procedure (`sp_refresh_monthly_summary`), since BigQuery Sandbox doesn't support DML (`INSERT`/`UPDATE`), only DDL (`CREATE OR REPLACE`).
   - `store_summary`: total revenue, total customers, and average ticket by store, used to rank top and bottom performers.
   - `alert_planning_variation`: flags store-weeks where sales dropped more than a defined threshold versus the prior week, including holiday context, so a planner can tell a real anomaly apart from an expected holiday dip.

A key correctness decision throughout: metrics like average ticket are always recalculated from underlying sums (`SUM(revenue) / SUM(customers)`), never averaged from a pre-computed ratio column — averaging an average silently produces wrong numbers once store sizes differ.

## Solution

The result is a small set of analytics-ready tables that answer the business questions directly, instead of requiring someone to re-derive them from raw data each time:

- `store_summary` → store ranking (top 5 / bottom 5 by revenue)
- `monthly_summary` → revenue, customers, and average ticket trend over time
- `alert_planning_variation` → early-warning table for sharp week-over-week sales drops, with holiday context to reduce false alarms

These tables are designed to plug directly into a BI tool (Looker Studio) for visualization.

## Business Impact

- **Faster anomaly detection**: instead of a planner manually scanning store performance, the alert table surfaces meaningful drops automatically, with holiday context included so a real anomaly isn't confused with an expected seasonal dip.
- **Reliable KPIs**: because metrics are built from validated, correctly-typed data and correct aggregation logic (sums, not averaged ratios), the numbers feeding any dashboard or report can be trusted.
- **Scalable design**: partitioning and clustering mean the same pipeline holds up as data volume grows, not just on a small sample.
- **Known limitation, already mapped**: native BI tools only support calendar-based period comparisons (month/quarter/year). Businesses that operate on custom cycles (e.g., promotional cycles of varying length) need this logic built into the SQL layer itself — a pattern I've identified here and plan to implement as a cycle-calendar dimension table in a future iteration of this case.

## Stack

- **SQL**: Google BigQuery (Standard SQL) — data quality checks, staging, partitioning/clustering, window functions, stored procedures
- **Data source**: [Rossmann Store Sales (Kaggle)](https://www.kaggle.com/c/rossmann-store-sales)
- **Visualization**: Looker Studio *(coming soon)*

## Files

- `demand_forecasting_rossmann.sql` — full pipeline script, from raw data to analytics-ready tables
