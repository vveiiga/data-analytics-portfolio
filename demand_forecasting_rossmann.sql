-- =====================================================================
-- DEMAND & SALES PERFORMANCE PIPELINE — ROSSMANN STORE SALES
-- =====================================================================
-- WHAT THIS DOES
-- End-to-end BigQuery pipeline for a retail demand-planning case: raw
-- data quality checks -> typed staging layer -> a partitioned/clustered
-- analytics table -> window-function trend metrics (7-day moving
-- average, week-over-week variation, daily store ranking) -> a monthly
-- summary table (kept fresh through a stored procedure), a per-store
-- revenue ranking, and a demand-drop alert table flagged against the
-- store's holiday calendar.
--
-- WHY IT MATTERS
-- Built to answer the questions a demand planner actually gets asked:
-- which stores are underperforming right now, is a sales drop a real
-- problem or just a holiday, and which store types/regions are driving
-- revenue. The output tables (monthly_summary, store_summary,
-- alert_planning_variation) feed a Looker Studio dashboard directly.
--
-- DATA SOURCE
-- Rossmann Store Sales (Kaggle): https://www.kaggle.com/c/rossmann-store-sales/data
--
-- ENVIRONMENT
-- Runs entirely on BigQuery Sandbox — no billing account required.
-- `sandbox-portfolio-vveiga` below is my personal sandbox project;
-- replace it with your own project ID to reproduce this.
--
-- TECHNIQUES COVERED
-- CREATE SCHEMA via SQL, SAFE_CAST / SAFE.PARSE_DATE data quality
-- checks, CREATE OR REPLACE TABLE, PARTITION BY + CLUSTER BY, window
-- functions (AVG/LAG/RANK OVER), a stored procedure, and CASE-based
-- business labeling.
-- =====================================================================
--- 1. CREATION OF SCHEMA, TABLES AND QUALITY CHECKS

-- AT FIRST I CREATE THE SCHEMA USING DIRECTLY THE SQL TO DO IT AND NOT USING THE BUTTON ON THE BQ STUDIO
-- I DEFINE THE STANDARD LOCATION OF THE US
CREATE SCHEMA IF NOT EXISTS `sandbox-portfolio-vveiga.raw`
OPTIONS (location = 'US');

-- AFTER THAT I DID THE INPUT MANUALLY OF THE FILES OF CSV TO THE SCHEMA TO CREATE THE TABLES.
-- ONE THAT SHOWS THE SALES ON VALUE AND OTHER THAT SHOWS SOME QUALITIES
-- I DID ALL THE INPUT AS RAW DATA USING STRING AS STANDARD DATA TYPE
-- SOON AFTER I DID SOME ANALYSIS TO SEE IF SOME COLUMNS HAS ANY TYPE VALUE THATS NOT EXPECTED
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(SAFE_CAST(id_store AS INT64) IS NULL AND NULLIF(id_store,'') IS NOT NULL) AS store_error,
  COUNTIF(SAFE.PARSE_DATE('%Y-%m-%d', date_sale) IS NULL AND NULLIF(date_sale,'') IS NOT NULL) AS date_error,
  COUNTIF(SAFE_CAST(sale_value AS INT64) IS NULL AND NULLIF(sale_value,'') IS NOT NULL) AS sales_error,
  COUNTIF(SAFE_CAST(qnty_customer AS INT64) IS NULL AND NULLIF(qnty_customer,'') IS NOT NULL) AS customers_error,
  COUNTIF(SAFE_CAST(flg_open AS INT64) IS NULL AND NULLIF(flg_open,'') IS NOT NULL) AS open_error,
  COUNTIF(SAFE_CAST(flg_promo AS INT64) IS NULL AND NULLIF(flg_promo,'') IS NOT NULL) AS promo_error,
  COUNTIF(SAFE_CAST(flg_state_holiday AS INT64) IS NULL AND NULLIF(flg_state_holiday,'') IS NOT NULL) AS stateholiday_error,
  COUNTIF(SAFE_CAST(flg_school_holiday AS INT64) IS NULL AND NULLIF(flg_school_holiday,'') IS NOT NULL) AS schoolholiday_error
FROM `sandbox-portfolio-vveiga.raw.tb_rossmann_store_sales`;

-- I FOUND IT SOME ERROR AT THE COLUMN STATE HOLIDAY THAT WE EXPECTED INTEGER VALUES BUT IT HAS SOME LETTERS
-- MOST OF THE VALUES ARE 0 BUT IT HAS A DISTRIBUTION OF VALUES THAT ARE LETTERS
SELECT
  flg_state_holiday,
  COUNT(*) AS count_values
FROM `sandbox-portfolio-vveiga.raw.tb_rossmann_store_sales`
--GROUP BY ALL 
-- GROUP BY 1
GROUP BY flg_state_holiday
;
-- THAT LETTER SHOWS SOME HOLIDAYS: 
-- 0: WITHOUT HOLIDAYS
-- A: PUBLIC HOLIDAYS
-- B: EASTER
-- C: CHRISTMAS
-- I LL PUT THE LINK DOWN BELOW THAT SHOWS SOME DETAILS OF THE DATA
-- https://www.kaggle.com/c/rossmann-store-sales/data

-- I CHECKED THIS SECOND DATASET AND AT THESE COLUMNS I DIDNT FOUND ANY TYPE PROBLEM AT THE VALUES

SELECT
  COUNT(*) AS store_total,
  COUNTIF(SAFE_CAST(id_store AS INT64) IS NULL AND NULLIF(id_store,'') IS NOT NULL) AS store_error,
  COUNTIF(SAFE_CAST(competition_distance AS FLOAT64) IS NULL AND NULLIF(competition_distance,'') IS NOT NULL) AS competitiondistance_error,
  COUNTIF(SAFE_CAST(promo AS INT64) IS NULL AND NULLIF(promo,'') IS NOT NULL) AS promo_error
FROM `sandbox-portfolio-vveiga.raw.tb_store`;

-- 2. STAGING

-- AT THIS POINT I TAKE THE DATA OF THE RAW TABLES AND I CREATE TRUSTED TABLES AT NEW SCHEMA TO USE THEM

CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.stg_train` AS
SELECT
  SAFE_CAST(id_store AS INT64) AS id_store,
  SAFE_CAST(day_week AS INT64) AS day_week,
  SAFE.PARSE_DATE('%Y-%m-%d', date_sale) AS date_sale,
  SAFE_CAST(sale_value AS INT64) AS sale_value,
  SAFE_CAST(qnty_customer AS INT64) AS qnty_customer,
  SAFE_CAST(flg_open AS INT64) AS flg_open,
  SAFE_CAST(flg_promo AS INT64) AS flg_promo,
  NULLIF(TRIM(flg_state_holiday), '') AS flg_state_holiday,
  SAFE_CAST(flg_school_holiday AS INT64) AS flg_school_holiday
FROM `sandbox-portfolio-vveiga.raw.tb_rossmann_store_sales`;

CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.stg_store` AS
SELECT
  SAFE_CAST(id_store AS INT64) AS id_store,
  NULLIF(TRIM(store_type), '') AS store_type,
  NULLIF(TRIM(assortment), '') AS assortment,
  SAFE_CAST(competition_distance AS FLOAT64) AS competition_distance,
  SAFE_CAST(competition_open_month AS INT64) AS competition_open_month,
  SAFE_CAST(competition_open_year AS INT64) AS competition_open_year,
  SAFE_CAST(promo AS INT64) AS promo,
  SAFE_CAST(promo_week AS INT64) AS promo_week,
  SAFE_CAST(promo_year AS INT64) AS promo_year,
  NULLIF(TRIM(promo_interval), '') AS promo_interval
FROM `sandbox-portfolio-vveiga.raw.tb_store`;

-- CHECKING IF THERE IS ANY NULL VALUE ON THAT COLUMNS BECAUSE I'LL USE THEM AS KEYS

SELECT
  COUNTIF(date_sale IS NULL) AS null_dates,
  COUNTIF(id_store IS NULL) AS null_stores
FROM `sandbox-portfolio-vveiga.analytics.stg_train`;

-- 3. PARTITION TABLE

-- NOW I CREATE THE TABLE TO HELP THE ANALYSIS AND PARTITION BY DATE_SALES AND CLUSTER BY STORE
-- I REMOVE THE DAYS WITHOUT SALES AND I JOIN THE DATA OF SALES WITH THE INFORMATIONS OF THE STORE

CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.sales_partitioned`
PARTITION BY date_sale
CLUSTER BY id_store AS
SELECT
  sale.id_store,
  sale.date_sale,
  sale.day_week,
  sale.sale_value,
  sale.qnty_customer,
  sale.flg_open,
  sale.flg_promo,
  sale.flg_state_holiday,
  sale.flg_school_holiday,
  store.store_type,
  store.assortment,
  store.competition_distance,
  store.promo as promo2
FROM `sandbox-portfolio-vveiga.analytics.stg_train` sale
LEFT JOIN `sandbox-portfolio-vveiga.analytics.stg_store` store
  ON sale.id_store = store.id_store
WHERE sale.flg_open = 1
  AND sale.date_sale IS NOT NULL
  AND sale.id_store IS NOT NULL;

-- 4. ANALYTICS PART
-- I USED WINDOW FUCTIONS TO CALCULATE THE AVERAGE OF SALE, THE SALE VARIATION AND THE DAILY RANKING PER STORE

CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.sales_with_trends` AS
SELECT
  *,
  AVG(sale_value) OVER (
    PARTITION BY id_store ORDER BY date_sale
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS moving_average_7d,

  sale_value - LAG(sale_value, 7) OVER (PARTITION BY id_store ORDER BY date_sale)
    AS variation_abs_vs_last_week,

  SAFE_DIVIDE(
    sale_value - LAG(sale_value, 7) OVER (PARTITION BY id_store ORDER BY date_sale),
    LAG(sale_value, 7) OVER (PARTITION BY id_store ORDER BY date_sale)
  ) AS pct_variation_vs_last_week,

  RANK() OVER (PARTITION BY date_sale ORDER BY sale_value DESC) AS ranking_stores_at_day
FROM `sandbox-portfolio-vveiga.analytics.sales_partitioned`;

-- 5. I CREATE THE TABLE TO SUPPORT THE DASHBOARD ON THE DATA STUDIO

CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.monthly_summary` AS
SELECT
  DATE_TRUNC(date_sale, MONTH) AS month,
  store_type,
  SUM(sale_value) AS total_revenue,
  ROUND(AVG(sale_value), 2) AS daily_average_sale,
  SUM(qnty_customer) AS total_customers,
  ROUND(SAFE_DIVIDE(SUM(sale_value), SUM( qnty_customer)), 2) AS average_ticket,
  COUNT(DISTINCT id_store) AS store_quantity
FROM `sandbox-portfolio-vveiga.analytics.sales_with_trends`
GROUP BY ALL;


-- 6. CREATE A STORED PROCEDURE TO THE MONTHLY SUMMARY

CREATE OR REPLACE PROCEDURE `sandbox-portfolio-vveiga.analytics.sp_refresh_monthly_summary`()
BEGIN
  CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.monthly_summary` AS
 SELECT
  DATE_TRUNC(date_sale, MONTH) AS month,
  store_type,
  SUM(sale_value) AS total_revenue,
  ROUND(AVG(sale_value), 2) AS daily_average_sale,
  SUM( qnty_customer) AS total_customers,
  ROUND(SAFE_DIVIDE(SUM(sale_value), SUM(qnty_customer)), 2) AS average_ticket,
  COUNT(DISTINCT id_store) AS store_quantity
FROM `sandbox-portfolio-vveiga.analytics.sales_with_trends`
GROUP BY ALL;
END;

-- IF NEED TO SCHEDULE I CAN SCHEDULE ONLY THE CALL OF THE PROCEDURE AND NOT ALL THE CODE
-- THAT MAKE IT EASY TO DO THE MODIFICATIONS ON THE PROCEDURE AND KEEP THE PREVIOUS VERSIONS TO SEE AT THE
-- RIGHT SIDE MENU OF THE BIG QUERY IF YOU DONT HAVE ACCESS TO THE GITHUB
-- THE CALL IS COMMENT DOWN BELOW

-- CALL `sandbox-portfolio-vveiga.analytics.sp_refresh_monthly_summary`();

-- 7. Table for a store Ranking
-- THAT QUERY I CREATE A TABLE TO SUPPORT THE CHART OF STORE RANKING
CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.store_summary` AS
SELECT
  id_store,
  store_type,
  SUM(sale_value) AS total_revenue,
  SUM(qnty_customer) AS total_customers,
  ROUND(SAFE_DIVIDE(SUM(sale_value), SUM(qnty_customer)), 2) AS average_ticket
FROM `sandbox-portfolio-vveiga.analytics.sales_with_trends`
GROUP BY id_store, store_type
ORDER BY total_revenue DESC;
-- 8. ALERT
-- AT THE END I CREATE AN ALERT THAT SHOWS YOU ANY STRONG VARIATION VS THE LAST WEEK
-- ---------------------------------------------------------------------
CREATE OR REPLACE TABLE `sandbox-portfolio-vveiga.analytics.alert_planning_variation` AS
SELECT
  id_store,
  date_sale,
  store_type,
  sale_value,
  ROUND(moving_average_7d, 1) AS moving_average_7d,
  ROUND(pct_variation_vs_last_week * 100, 1) AS pct_variation_vs_last_week,
  CASE flg_state_holiday
    WHEN 'a' THEN 'Public holiday'
    WHEN 'b' THEN 'Easter'
    WHEN 'c' THEN 'Christmas'
    ELSE 'No holiday'
  END AS holiday_context
FROM `sandbox-portfolio-vveiga.analytics.sales_with_trends`
WHERE pct_variation_vs_last_week < -0.20   -- NEGATIVE VARIATION OF 20% vs. LAST WEEK
ORDER BY date_sale DESC;

-- THAT TABLES ANSWER THE QUESTION:
-- "WHICH STORES HAD A NEGATIVE VARIATION AGAINST LAST WEEK
--  AND SHOWS THAT MAYBE THE PLANNING NEEDS TO ANALYSIS(OUT OF STOCK ISSUE, LOCAL ISSUE, COMPETITION RISE OF SALES)?"
