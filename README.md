# Ride-Hailing Trip Analytics Platform (Lakeflow Spark Declarative Pipelines)

![Databricks](https://shields.io)
![Unity Catalog](https://shields.io)
![SQL](https://shields.io)
![BI](https://shields.io)

## 1. Executive Summary
A city ride-hailing company operating across eight Indian cities exports its completed trip records to a monthly CSV file (`ride_trips_raw.csv`). Previously, reporting was done manually in Excel, leading to slow turnaround times and errors caused by duplicate rows, inconsistent capitalization, leading/trailing spaces, missing fields, and invalid distances or rates.

This project implements an automated, one-click **Databricks Medallion Architecture** using **Lakeflow Spark Declarative Pipelines**, **Unity Catalog**, and **Databricks AI/BI Dashboards**. It ingests raw trip exports, cleans and validates records across Bronze, Silver, and Gold layers, proves row-level data reconciliation, and delivers a 3-page executive dashboard.

---

## 2. Business Objectives & Requirements Met
* **BR-01 (Monthly Trends):** Track monthly trips, total distance (km), net revenue, driver payouts, and platform earnings.
* **BR-02 (City & Region Performance):** Compare trip volume, net revenue, and average fare across 8 cities and 4 regions.
* **BR-03 (Vehicle Performance):** Analyze trips, distance, revenue, and customer ratings across Bike, Auto, Mini, Sedan, and Premium categories.
* **BR-04 (Payment Analysis):** Monitor trip distribution, revenue, and pending payments across UPI, Cash, Card, and Wallet.
* **BR-05 (Data-Quality Visibility):** Audit exact counts of duplicates, missing values, and invalid numeric records.
* **BR-06 (Row-Level Reconciliation):** Mathematically verify that zero rows are silently dropped (`Bronze = Clean + Rejected + Duplicates`).
* **BR-07 (Repeatability):** Execute the entire end-to-end transformation with a single click in Lakeflow Pipelines.
* **BR-08 (Executive Dashboard):** Visualize all KPIs and data quality metrics in a published 3-page Databricks AI/BI Dashboard.

---

## 3. Medallion Architecture & Data Flow

```text
[ Unity Catalog Volume: /Volumes/mobility_catalog/trips/raw_files/ride_trips_raw.csv ]
                                          │
                                          ▼ (Incremental Streaming Read)
                        ┌───────────────────────────────────┐
                        │  BRONZE: bronze_ride_trips (1,214) │
                        └─────────────────┬─────────────────┘
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    ▼ (Valid & Deduplicated)                    ▼ (Quarantined)
   ┌─────────────────────────────────┐         ┌─────────────────────────────────┐
   │ SILVER: silver_trips_clean      │         │ SILVER: silver_trips_rejected   │
   │ (1,168 clean rows)              │         │ (22 rows + reject_reason)       │
   └────────────────┬────────────────┘         └────────────────┬────────────────┘
                    │                                           │
     ┌──────────────┼──────────────┬──────────────┐             │
     ▼              ▼              ▼              ▼             ▼
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐
│   GOLD   │  │   GOLD   │  │   GOLD   │  │   GOLD   │  │     GOLD      │
│ Monthly  │  │   City   │  │ Vehicle  │  │ Payment  │  │ Data Quality  │
│  Trips   │  │ Summary  │  │ Summary  │  │ Summary  │  │ & Reconcile   │
└────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └───────┬───────┘
     └─────────────┴─────────────┴─────────────┴────────────────┘
                                 │
                                 ▼
              [ 3-Page Databricks AI/BI Dashboard ]
```

---

## 4. Data Cleaning & Validation Rules (Silver Layer)

| Data Issue | Raw Example | Cleaning Rule Applied | SQL Implementation |
| :--- | :--- | :--- | :--- |
| **Duplicate Trips** | Same `trip_id` repeated | Keep one unique record per trip | `SELECT DISTINCT ...` |
| **Extra Spaces & Mixed Case** | `" mumbai"`, `"SEDAN"`, `" app "` | Trim whitespace and convert to Title Case | `INITCAP(TRIM(col))` |
| **Acronym Preservation** | `"UPI"` in `payment_method` | Trim whitespace only (skip `INITCAP` so `UPI` doesn't become `Upi`) | `COALESCE(TRIM(payment_method), 'Unknown')` |
| **Blank City / Payment** | `city` is `NULL` | Replace missing categories with `'Unknown'` | `COALESCE(INITCAP(TRIM(city)), 'Unknown')` |
| **Blank Fare Amount** | `fare_amount` is `NULL` | Recalculate using distance × rate per km | `COALESCE(fare_amount, distance_km * rate_per_km)` |
| **Blank Rating / Customer** | `customer_rating` is `NULL` | Keep row and preserve `NULL` so `AVG()` is not skewed by zeros | Leave as `CAST(customer_rating AS INT)` |
| **Invalid Distance / Rate** | `distance_km <= 0` or `rate_per_km < 0` | Drop from clean table via Expectation; route to `silver_trips_rejected` | `EXPECT (distance_km > 0) ON VIOLATION DROP ROW` |

### Key Business Formulas
* **Fare Amount:** distance\_km × rate\_per\_km
* **Net Revenue:** fare\_amount - discount\_amount
* **Platform Earnings:** net\_revenue - driver\_payout

---

## 5. Repository Structure

```text
ride-hailing-analytics-platform/
├── README.md                     # Project documentation and setup guide
├── sql/
│   ├── 01_setup_storage.sql      # Catalog, schema, and Unity Catalog Volume creation
│   └── my_transformation.sql     # Complete Lakeflow Pipeline (Bronze, Silver, Gold)
├── data/
│   └── ride_trips_raw.csv        # Synthetic raw trip dataset (1,214 rows, 20 columns)
└── screenshots/
    ├── pipeline_graph.png        # Lakeflow DAG execution graph
    ├── gold_data_quality.png     # Reconciliation verification output
    ├── dashboard_page1_overview.png
    ├── dashboard_page2_vehicles_payments.png
    └── dashboard_page3_data_quality.png
```

---

## 6. Setup & Execution Guide

### Step 1: Create Unity Catalog Storage
Run the following commands in a Databricks SQL Editor to provision storage:
```sql
CREATE CATALOG IF NOT EXISTS mobility_catalog;
CREATE SCHEMA IF NOT EXISTS mobility_catalog.trips;
CREATE VOLUME IF NOT EXISTS mobility_catalog.trips.raw_files;
```

### Step 2: Upload Raw Data
1. Open **Catalog Explorer** in Databricks and navigate to `mobility_catalog` → `trips` → `raw_files`.
2. Upload `ride_trips_raw.csv` into the volume.

### Step 3: Configure & Run the Lakeflow Pipeline
1. Go to **Workflows** (or **Data Engineering**) → **Pipelines** → **Create Pipeline**.
2. Name the pipeline `ride_hailing_etl`, select **Serverless** compute, set Default Catalog to `mobility_catalog`, and Target Schema to `trips`.
3. Add the Bronze, Silver, and Gold table definitions inside `my_transformation.sql`.
4. Click **Run pipeline** to execute the full DAG.

### Step 4: Verify Reconciliation
Run a SQL query against the Gold audit table to confirm zero data loss:
```sql
SELECT * FROM mobility_catalog.trips.gold_data_quality;
```
**Expected Reconciliation Output:**
* `bronze_rows`: **1,214**
* `clean_rows`: **1,168**
* `rejected_rows`: **22**
* `duplicate_rows`: **24**
* `reconciliation_passed`: **Yes** ($1,168 + 22 + 24 = 1,214$)

---

## 7. Dashboard Overview (Databricks AI/BI)
The published dashboard connects exclusively to the five `gold_*` tables and includes interactive filters for **Month**, **Region**, and **Vehicle Type**:

1. **Page 1 — Overview:** Executive KPIs (Total Trips, Net Revenue, Total Distance, Platform Earnings), monthly net revenue trend line, net revenue by city, and trips by vehicle type.
2. **Page 2 — Vehicles & Payments:** Trip volume and net revenue by vehicle type, average customer rating by vehicle type, payment method breakdown, and total pending payment amount.
3. **Page 3 — Data Quality:** Reconciliation KPI cards (`1,214` Bronze vs. `1,168` Clean), pass/fail status indicator, and a breakdown chart of every data anomaly detected in the raw file.

---

## 8. Key Engineering Takeaways
* **Immutability in Bronze:** Keeping raw files untouched with ingestion metadata (`source_file`, `loaded_at`) ensures full auditability and allows bug fixes in downstream Silver transformations without re-requesting source exports.
* **Quarantine Over Silent Deletion:** Routing corrupt rows to `silver_trips_rejected` with explicit `reject_reason` labels prevents KPI distortion while giving upstream engineering teams actionable feedback.
* **Gold-Only BI Serving:** Restricting dashboards to pre-aggregated Gold tables eliminates the risk of business users accidentally querying uncleaned data and guarantees fast dashboard load times.
