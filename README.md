<div align="center">

#  UrbanSurge
### Enterprise Urban Demand Forecasting & Algorithmic Fleet Optimization Engine

*A high-throughput ML Systems & Data Infrastructure platform modeling **39.2 million real NYC taxi trips** across 39,033 spatio-temporal cells. Integrates an incremental Parquet analytical lakehouse, dual-engine DuckDB/PySpark verification, disciplined time-series forecasting, and a capacity-constrained bounded min-heap dispatch optimizer achieving **+15.98% revenue lift**.*

<p align="center">
  <a href="https://Akgithub2028.github.io/UrbanSurge/"><img src="https://img.shields.io/badge/%E2%96%B6%20Interactive-Live%20Demo-2ea44f?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Live Demo" /></a>
  <a href="https://render.com/deploy?repo=https://github.com/Akgithub2028/UrbanSurge"><img src="https://img.shields.io/badge/%E2%87%A7%20Deploy-on%20Render-46E3B7?style=for-the-badge&logo=render&logoColor=black" alt="Deploy to Render" /></a>
  <a href="https://urbansurge.onrender.com"><img src="https://img.shields.io/badge/%E2%97%86%20Live-FastAPI%20Endpoint-792ee5?style=for-the-badge&logo=fastapi&logoColor=white" alt="Live API" /></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/DuckDB-In--Process%20OLAP-FFF000?style=flat-square&logo=duckdb&logoColor=black" alt="DuckDB" />
  <img src="https://img.shields.io/badge/PySpark-4.2.0%20Distributed-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="PySpark" />
  <img src="https://img.shields.io/badge/LightGBM-GBDT%20Regressor-02569B?style=flat-square&logo=scikitlearn&logoColor=white" alt="LightGBM" />
  <img src="https://img.shields.io/badge/FastAPI-High%20Throughput-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI" />
  <img src="https://img.shields.io/badge/Docker-Multi--Stage%20Container-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker" />
  <img src="https://img.shields.io/badge/GitHub%20Actions-CI%2FCD%20Gates-2088FF?style=flat-square&logo=githubactions&logoColor=white" alt="CI/CD" />
  <img src="https://img.shields.io/badge/Tests-48%20Passed-success?style=flat-square&logo=pytest&logoColor=white" alt="Tests" />
  <img src="https://img.shields.io/badge/Scale-39.2M%20Trips%20%C2%B7%20$1.1B%20Gross-F7931E?style=flat-square" alt="Scale" />
  <img src="https://img.shields.io/badge/License-MIT-blue?style=flat-square" alt="License" />
</p>

</div>

---

## 🏗️ Architecture & Engineering Pillars

UrbanSurge addresses a fundamental operations question: **where should an urban fleet be dispatched hour-by-hour to maximize driver earnings under physical demand constraints?**

```
 41.2M Raw TLC Trips (0.69 GB Parquet)
                  │
                  ▼
   [ Data Quality & Anomaly Gate ] ──────> Exclusion triage: NULL passenger_count recovery
                  │                       (+4.1M valid rides salvaged)
                  ▼
   [ Partitioned Lakehouse Marts ] ──────> Content-hash watermarking & Hive partitions
         │                  │
         ▼                  ▼
    DuckDB (OLAP)      PySpark 4.2.0      ───> Dual-Engine Verification (Exact match to the cent)
  (844ms Mart Build)  (Broadcast + Shuffle)
         │
         ▼
   [ Time-Series & Feature Engine ] ────> 168h Seasonal Decomposition (Periodicity = 1.0000)
         │                                Cyclical sin/cos encodings (hour/dow)
         ▼
   [ Model Selection & Baseline Gate ] ──> Arithmetic Historical Mean vs LightGBM (MAE 2.6699 vs 2.6763)
         │
         ▼
   [ Bounded Min-Heap Optimizer ]  ────> O(n log k) Top-k Dispatch + 1.5x Greedy Capacity Fill
         │                                (+15.98% Mean Lift over trip volume baseline)
         ▼
   [ Production Serving Tier ]     ────> FastAPI REST API + Client-Side WebAssembly/JS Simulator
```

---

## 📊 Pipeline Scale & Ingestion Telemetry

Every metric below is measured from end-to-end execution and verified in `outputs/`:

| Dimension | Measured Metric | Architectural Detail |
|---|---|---|
| **Raw Ingestion** | **41,169,720 trips** across 12 monthly Parquet files (0.69 GB) | NYC TLC Yellow-Cab, public public-domain records |
| **Warehouse Scale** | **39,203,425 validated trips** (4.78% exclusion rate) | Partitioned by month with SHA-256 content watermarks |
| **Gross Modeled Volume** | **$1,103,993,060 revenue** across 11,031,247 driver-hours | Division of aggregate sums (never averages of ratios) |
| **Spatio-Temporal Grid**| **39,033 discrete cells** (Zone × Day-of-Week × Hour) | Complete 168-hour calendar spine cross-joined |
| **Reconciliation Gates**| **9 / 9 integrity checks passed** | Build aborts non-zero on any metric discrepancy |

---

## 🔍 Core Findings & Engineering Decisions

### 1. Data-Centric ML: A Quality Rule Silently Discarded an 1/8th of All Demand

Initial pipeline versions rejected **14.71%** of all trips. Analyzing the exclusion taxonomy revealed an alarming distribution:

```text
implausible_passengers   12.66%   <- 70% of ALL exclusions
implausible_distance      1.68%
non_positive_fare         1.45%
implausible_duration      1.26%
unknown_zone              0.88%
```

The rule had categorized `NULL passenger_count` as invalid. In a single month, this discarded **483,731 trips** — of which **89.5% were legitimate trips** with valid meters, fares, zones, and GPS coordinates (certain taxi vendors simply omit the passenger counter).

Crucially, this data loss was non-random: these trips had a **mean distance of 20.11 miles** (vs. 3.0 miles fleet average) — heavily representing airport runs. Silently dropping them had systematically biased revenue projections downward in precisely the highest-yield dispatch zones.

| Metric | Before Gate Fix | After Gate Fix | Recovery Impact |
|---|---:|---:|---|
| **Exclusion Rate** | 14.71% | **4.78%** | **-9.93% reduction** |
| **Validated Trips** | 35,112,645 | **39,203,425** | **+4,090,780 real trips recovered** |

*CI Gate*: A regression check permanently asserts `excluded_pct < 10.0%`.

---

### 2. Algorithmic Dispatch: +16% Lift & Non-Linear Capacity Saturation

Allocations are built using training data (**Jan–Sep**) and evaluated against held-out ground truth (**Oct–Dec** observed revenue rates) with hard cell capacity ceilings.

| Fleet Size | Revenue Policy ($) | Trip-Volume Policy ($) | Uniform Baseline ($) | Lift vs Volume Baseline | Lift vs Uniform |
|---:|---:|---:|---:|---:|---:|
| **100 drivers** | $2,169,503 | $1,842,185 | $1,448,374 | **+17.77%** | +49.79% |
| **300 drivers** | $6,440,931 | $5,425,622 | $3,150,718 | **+18.71%** | +104.43% |
| **500 drivers** | $10,371,293 | $8,939,127 | $5,218,345 | **+16.02%** | +98.75% |
| **1,000 drivers**| $19,158,828 | $17,194,139 | $8,018,815 | **+11.43%** | +138.92% |

* **Mean revenue lift**: **+15.98%** over volume-ranking and up to **+138.92%** over uniform dispersion.
* **Diminishing Returns (Saturation Decay)**: Lift drops from +18.7% to +11.4% as fleet size expands. High-yield airport and central business district cells saturate their capacity ceiling, forcing incremental drivers into marginal zones. Algorithmic optimization is most valuable to capital-constrained, smaller fleets.

---

### 3. Model Discipline: Arithmetic Historical Mean Beats Gradient Boosted Trees

Rather than blindly deploying complex machine learning, the candidate model (LightGBM with cyclical sin/cos hour and weekday encodings) was pitted against an honest baseline — the historical cell mean:

| Model Architecture | MAE | RMSE | R² | Verdict |
|---|---:|---:|---:|---|
| **Historical Cell-Mean Baseline** | **2.6699** | **8.2072** | **0.9810** | **Selected for Production** |
| **LightGBM (300 Trees, Depth 63)**| 2.6763 | 8.2561 | 0.9808 | Rejected (+0.24% error) |

* **The Cause**: Classical additive seasonal decomposition reveals **seasonal strength = 1.0000** over the 168-hour weekly cycle (a 446,142 trip delta between Thursday 18:00 peak and Tuesday 03:00 trough). The variance is almost completely captured by `(zone, day_of_week, hour)`. 
* **Engineering Philosophy**: *The model is the payload, never the point.* Shipping an over-parameterized model when arithmetic performs better adds unnecessary latency and maintenance overhead.

---

### 4. Market Microstructure: Volume Does Not Equal Value

| Zone Metric | Zone Name | Total Trips | $/Driver-Hour Yield |
|---|---|---:|---:|
| **Busiest Zone** | Upper East Side South | 1,862,329 | $101.84 / hr |
| **Most Valuable Zone** | **LaGuardia Airport** | 1,233,244 | **$128.39 / hr** |

* **The Top 12 zones by volume and top 12 by value share only 4 zones.**
* Traditional dispatch dashboards optimize for "where rides happen," sending fleets into dense crosstown short hops. UrbanSurge optimizes for net $/driver-hour, capturing high-yield airport transit and accounting for deadheading penalties.

---

## ⚡ ML Infra & Data Platform Details

### Dual-Engine Distributed Validation: PySpark vs DuckDB
To guarantee analytical accuracy, the entire aggregation was implemented in two independent engines:
1. **PySpark 4.2.0** (`etl/spark_job.py`): Explicit broadcast join for the 263-row zone dimension table, pre-aggregation before joins to minimize shuffle volume, and partition tuning (`spark.sql.shuffle.partitions = 8`).
2. **DuckDB In-Process OLAP**: Columnar vectorized processing over Parquet partitions.

```text
Validation Telemetry (Spark vs DuckDB):
--------------------------------------------------------------
[PASS] trips          Spark: 9,101,490      DuckDB: 9,101,490
[PASS] cells          Spark: 32,060         DuckDB: 32,060
[PASS] revenue        Spark: 246,696,744.98 DuckDB: 246,696,744.98
[PASS] driver_hours   Spark: 2,350,332.3489 DuckDB: 2,350,332.3489
```

* **Speedup**: DuckDB executes the full aggregation in **844 ms** (**56× faster** than PySpark on single-node execution, completing before the JVM fully boots).

### Algorithmic Allocation Complexity
* **Bounded Min-Heap Top-K**: Instead of sorting all $N=39,033$ cells in $O(N \log N)$, candidates are streamed through a bounded min-heap of size $k$ in **$O(N \log k)$ time and $O(k)$ memory**.
* **Greedy Capacity Constraints**: Allocates up to `capacity = observed_trips_per_hour * 1.5` per cell. Because driver-hours are homogeneous and capacities are hard constraints, greedy fill is provably optimal.

---

## 📂 Repository Topology

```text
urban-demand-warehouse/
├── etl/
│   ├── incremental.py     # Partitioned Parquet loader with SHA-256 watermarks
│   ├── quality.py         # Single-pass categorical quality gate
│   └── spark_job.py       # Distributed PySpark implementation with broadcast joins
├── sql/
│   ├── 01_dim_zone.sql    # Spatial zone dimension table
│   ├── 02_fact_trip.sql   # Filtered & typed trip facts
│   ├── 03_mart_demand.sql # Zone × dow × hour demand aggregation
│   └── 04_mart_revenue.sql# Revenue per driver-hour mart
├── src/
│   ├── allocate.py        # Bounded min-heap allocator & greedy capacity fill
│   ├── backtest.py        # Temporal train/test simulation & lift verification
│   ├── forecast.py        # LightGBM GBDT regressor vs arithmetic baseline
│   ├── seasonal.py        # 168-hour classical additive seasonal decomposition
│   └── build.py           # Orchestrator with 9 reconciliation gates
├── dashboard/
│   ├── app.py             # FastAPI REST endpoints & in-memory simulation
│   └── index.html         # Interactive heatmap and fleet slider UI
├── docs/                  # Client-side simulator hosted on GitHub Pages
├── infra/                 # Dockerfile (multi-stage build) and Render/Fly specs
└── tests/                 # 48 unit and integration tests
```

---

## 🚀 Quickstart & Reproduction

```bash
# 1. Environment Setup
git clone https://github.com/Akgithub2028/UrbanSurge.git
cd UrbanSurge
make setup

# 2. Complete End-to-End Pipeline Execution
# (Fetches data -> Runs ETL -> Builds Warehouse -> Backtests -> Forecasts -> Runs 48 Tests)
make all

# 3. Launch the Live API and Interactive Dashboard
make serve
# -> Serving at http://localhost:8500
```

### Automated CI/CD Regression Gates (`.github/workflows/ci.yml`)
1. **Logic Gate**: Standalone pytest validation of the bounded heap allocator and quality filters with zero network dependencies.
2. **Pipeline Gate**: End-to-end ingestion of 3 months of TLC Parquet, asserting:
   - Data exclusion rate `< 10.0%`
   - Out-of-sample backtest lift `> 5.0%`
   - Full 9/9 reconciliation checks
3. **Distributed Engine Gate**: Validates PySpark output against DuckDB to the cent.

---

## ⚖️ Known Limits & Engineering Trade-Offs

- **Origin-Side Demand Only**: Demand is computed based on pickup locations. Repositioning cost across trips is not modelled (a global time-expanded network flow would be required).
- **Inelastic Pricing**: The backtest assumes historical fare rates remain stationary regardless of added supply; in reality, sending 500 cars to one cell would induce price compression.
- **TLC Yellow Fleet Scope**: Excludes green cabs and for-hire vehicle (FHV/Uber/Lyft) records.

---

<div align="center">
  <sub>Authored and maintained by <b><a href="https://github.com/Akgithub2028">Aayaann Kausar</a></b> under the MIT License.</sub>
</div>
