# Ride-Hailing Trip Analytics Platform (Lakeflow Spark Declarative Pipelines)

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

## 8. Key Engineering Takeaways & Design Decisions
* **Immutability in Bronze (`bronze_ride_trips`):** Keeping raw CSV records untouched with ingestion metadata (`source_file` and `loaded_at`) ensures full auditability. If a bug is discovered in cleaning rules later, the pipeline can be re-run without asking the source system to re-export the file.
* **Quarantine Over Silent Deletion (`silver_trips_rejected`):** Instead of silently dropping corrupt rows (`distance_km <= 0` or `rate_per_km < 0`), isolating them in a rejected table with a clear `reject_reason` keeps Gold revenue metrics accurate while giving engineering teams an audit trail to fix upstream app bugs.
* **Smart Text & Null Handling:**
  * Applying `TRIM()` before `INITCAP()` cleans leading spaces so `" mumbai"` becomes `"Mumbai"`, while skipping `INITCAP()` on `payment_method` keeps acronyms like `"UPI"` uppercase instead of turning them into `"Upi"`.
  * Leaving blank `customer_rating` values as `NULL` instead of `0` prevents artificial distortion of average driver ratings, since SQL's `AVG()` automatically skips `NULL`s.
* **End-to-End Reconciliation (`gold_data_quality`):** Proving that `1,214 Bronze rows = 1,168 Clean + 22 Rejected + 24 Duplicates` mathematically guarantees that zero rows were lost during transformation.
* **Gold-Only BI Serving:** Restricting the Databricks AI/BI dashboard strictly to pre-aggregated Gold tables prevents business users from accidentally querying uncleaned data and ensures fast dashboard performance.

## 9. Project Review Q&A

**1. Why do we keep the raw data in Bronze instead of cleaning the file directly?**
* Keeping an untouched copy of the raw data in Bronze with ingestion metadata (`source_file` and `loaded_at`) creates an immutable audit trail. If a bug is discovered in our Silver cleaning logic later, we can fix the SQL rule and re-run the pipeline without needing the source system to re-export the file.

**2. What is the difference between removing a trip and putting it in the rejected table?**
* Removing a trip deletes it without a trace, making it impossible to know why data went missing. Routing invalid rows (`distance_km <= 0` or `rate_per_km < 0`) to `silver_trips_rejected` with a `reject_reason` keeps Gold revenue numbers accurate while giving engineering teams visibility to fix upstream app bugs.

**3. Why can `trip_id` find duplicates while `customer_id` cannot?**
* `trip_id` represents one unique completed ride, so any repeated `trip_id` is a true duplicate record. In contrast, `customer_id` repeats naturally whenever the same customer books multiple rides over time.

**4. Why do we use `TRIM` before `INITCAP`, and why not on `payment_method`?**
* Leading spaces confuse `INITCAP` if not removed first—running `TRIM` first turns `" mumbai"` into `"mumbai"`, allowing `INITCAP` to cleanly produce `"Mumbai"`. We skip `INITCAP` on `payment_method` because it would turn uppercase acronyms like `"UPI"` into `"Upi"`.

**5. Why do we leave a blank rating as `NULL` instead of replacing it with `0`?**
* Replacing a missing rating with `0` would falsely count it as a terrible trip and artificially drag down average driver and vehicle ratings. SQL's `AVG()` function automatically ignores `NULL` values, keeping rating averages accurate.

**6. How does the reconciliation check prove that no row was lost?**
* Every input row entering Bronze (`1,214` rows) must end up in one of three exact buckets: valid rows in `silver_trips_clean` (`1,168`), invalid rows quarantined in `silver_trips_rejected` (`22`), or exact duplicate rows removed (`24`). Confirming `1,214 = 1,168 + 22 + 24` proves 100% row-level accounting.

**7. Which vehicle type earns the most, and which has the best rating?**
* *(Check `gold_vehicle_summary` or Page 2 of your dashboard and insert your exact values here—for example: **Sedan / Premium** generated the highest net revenue, while **Premium / Sedan** achieved the highest average customer rating.)*

**8. Which dashboard tool did you choose, and why?**
* I chose **Databricks AI/BI Dashboards** because it connects natively to Unity Catalog Gold tables with zero external connectors, supports built-in Genie natural-language visualization generation, and keeps the entire pipeline and reporting workflow inside one unified platform.
