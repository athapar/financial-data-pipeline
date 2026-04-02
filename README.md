# Financial Data Pipeline
![Architecture](./docs/architecture_preview.png)
## 1. Project Summary
This project is a batch-oriented financial data pipeline that constructs point-in-time correct datasets using SCD2 identity modeling and explicit corporate adjustments. The pipeline ingests raw market data from Polygon (bars, splits, tickers) and builds a structured warehouse in BigQuery using dbt. 

The core problem addressed is that vendor data alone is not sufficient for historical correctness: ticker changes occur and break identity, and split events distort price continuity. This pipeline resolves both by maintaining a slowly changing security master (SCD2 on `composite_figi`) and applying cumulative split adjustment factors to raw prices.

The output is an analytics-ready dataset at the security level (FIGI + date) that supports consistent historical analysis, including returns and volatility metrics.

## 2. Architecture

The system separates ingestion, storage, and transformation to enforce correctness and reproducibility. Raw data is first written to canonical parquet datasets, then loaded into an append-only BigQuery raw layer free of business logic. 

![Architecture](./docs/architecture.png)

Transformations are implemented in dbt across staging, intermediate, and mart layers. The `composite_figi` is used instead of `ticker` to maintain identity across time, and an SCD2 snapshot tracks changes in ticker metadata. Split adjustments are computed explicitly in an intermediate layer and applied downstream to produce point-in-time correct prices. This layered design ensures idempotent ingestion, deterministic transformations, and reproducible analytical outputs. 


## 3. Key Design Decisions
* **Unadjusted prices + explicit adjustment layer**
  Raw OHLCV data is stored unmodified, and split adjustments are applied downstream using cumulative factors. This avoids reliance on vendor-adjusted data, which is often opaque and inconsistent across providers.

* **SCD2 security master (composite FIGI)**
  Tickers are not stable identifiers (e.g. FB → META). A slowly changing dimension keyed on `composite_figi` preserves security identity over time and enables correct point-in-time joins.

* **Idempotent ingestion**
  Ingestion is designed to support safe reruns and overlapping backfills via dedup on `(ticker, date)`. This ensures that the pipeline is repeatable and does not introduce data drift.

* **Intermediate layer separation**
  Business logic such as split adjustment is isolated in the intermediate layer instead of staging or fact models. This keeps transformations testable and easier to validate.


## 4. Data Quality Guarantees

Data quality is enforced through dbt tests at multiple layers:

* **Schema tests** enforce:

  * Not null constraints on key fields
  * Uniqueness at grain level (e.g. `composite_figi + date`)

* **Custom tests** validate:

  * SCD2 integrity (no overlapping validity windows per FIGI)
  * Correct grain enforcement in fact tables
  * No duplicate records after transformations

15 tests currently pass across staging, intermediate, and mart layers, ensuring that the dataset is structurally and temporally consistent.

## Quick Start

### 1. Configure `.env`

```env
POLYGON_API_KEY=YOUR_API_KEY
GOOGLE_CLOUD_PROJECT=GOOGLE_CLOUD_PROJECT_ID
BQ_DATASET_ID=BIG_QUERY_DATASET_ID
GOOGLE_APPLICATION_CREDENTIALS=PATH_TO_CREDENTIALS_JSON
```
### 2. Set up import list
Add tickers to import in `symbols.txt` for universe of stocks.

### 3. Run tests
```bash
python -m pytest -q
cd warehouse && dbt test
```

### 4. Run Pipeline
```bash
# Run the full pipeline
cd flows
python -m pipeline_flow
```

## Repository Structure
```text
src/
  financial_data_pipeline/
    cli.py
    config.py
    load_bq.py
    orchestrator.py
    polygon.py
    transforms.py
flows/
  pipeline_flow.py

warehouse/
  staging/
  intermediate/
  marts/
  snapshots/
  tests/

data/
  raw parquet datasets (per symbol)

docs/
  architecture diagrams

tests/
  pytest coverage for ingestion + validation
  ```

## Known Gaps + Roadmap

* **SCD2 historical backfill gap**
  * Current snapshots are forward-looking from initial load. Full historical reconstruction of security master state is not yet implemented.

* **Corporate actions expansion**
  * Only split adjustments are modeled. Dividends and total return adjustments are not yet included.

* **Macro data integration (FRED)**
  * Planned but not yet integrated into downstream marts.

* **Incremental dbt models**
  * Current models run in full-refresh mode; incremental strategies are planned for scalability.
