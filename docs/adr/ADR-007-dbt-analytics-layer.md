# ADR-007: dbt Analytics Layer for Gold Transformations

**Date:** 2026-06-05  
**Status:** Accepted  
**Supersedes:** None  

## Context
After establishing the Bronze (MongoDB) and Silver (Supabase) database storage abstraction paths, the platform needed:
- Analytics-ready aggregated data vectors (Gold layer)
- Version-controlled database transformation logic
- Automatic documentation of historical data lineage
- Structural testing frameworks for target data quality validation

The current Silver layer contains 44,501 validated sensor logs but lacks:
- Aggregations (hourly, daily operational sliding windows)
- Derived categories (sensor_type string prefix extractions)
- Business-ready analytics views optimized for consumption

## Decision
**Implement dbt (data build tool) as the dedicated platform data transformation core.**

### Why dbt?
| Factor | dbt | Alternatives (Raw SQL Scripts) |
| :--- | :--- | :--- |
| **Version Control** |  Native compilation logic in Git |  Manual layout tracking |
| **Documentation** |  Auto-generated schema catalogs |  None |
| **Lineage Tracking** |  Visual DAG dependencies |  Manual map graphing |
| **Testing Hooks** |  Built-in constraints parsing |  Custom execution blocks |
| **Industry Standard**|  High portability ecosystem |  Legacy pattern fragmentation |

## Implementation Details

### Environment Setup
```yaml
# dbt_project.yml
name: 'edge_platform'
version: '1.0.0'
config-version: 2

profile: 'edge_platform'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  edge_platform:
    silver:
      +materialized: table
    gold:
      +materialized: table
```

## Source Configuration

```yaml
# models/silver/src_supabase.yml
version: 2

sources:
  - name: supabase_source
    description: "Production transactional pooler data engine"
    database: postgres
    schema: dataops
    tables:
      - name: silver_sensor_data
        description: "Validated sensor readings received via Airflow/C-extension parser"
```

## Silver Layer Model

```sql
-- models/silver/clean_sensor_data.sql
{{ config(materialized='table') }}

SELECT 
    sensor_id,
    "value",
    unit,
    checksum,
    validated,
    source_timestamp,
    batch_id,
    CASE 
        WHEN sensor_id LIKE 'temp%' THEN 'Temperature'
        WHEN sensor_id LIKE 'pressure%' THEN 'Pressure'
        WHEN sensor_id LIKE 'flow%' THEN 'Flow'
        ELSE 'Other'
    END AS sensor_category
FROM {{ source('supabase_source', 'silver_sensor_data') }}
```

## Gold Layer Model

```sql
-- models/gold/hourly_metrics.sql
{{ config(materialized='table') }}

SELECT 
    date_trunc('hour', source_timestamp) AS hourly_window,
    sensor_category,
    AVG("value") AS avg_value,
    COUNT(*) AS reading_count
FROM {{ ref('clean_sensor_data') }}
GROUP BY 1, 2
```

## Technical Challenges & Resolutions

| Challenge Encountered | Root Cause Diagnosis | Engineering Resolution |
|---|---|---|
| mashumaro Serialization Error | Python 3.12/3.14 alpha dependency loop on internal `"schema"` fields inside dbt-common. | Forced specific decoupled override installation of `mashumaro==3.17.0` using custom command flags. |
| Pydantic V2 Conflict | dbt-core 1.10.0 uses legacy V1 BaseSettings location, conflicting with installed V2 wheels. | Applied a dynamic intercept in `sitecustomize.py` mapping classes at runtime activation. |
| PostgreSQL Syntax Loops | PowerShell `Set-Content` added a hidden Byte Order Mark (BOM) (EF BB BF) breaking the Jinja parser. | Programmed an inline execution statement using standard Python native `open(..., encoding='utf-8')` write blocks. |
| Reserved Keyword Errors | `value` column string triggered Postgres engine parsing exceptions. | Explicitly escaped properties inside SQL queries using standard double-quotes (`"value"`). |
| Missing Columns | Model looked for a tracking field (`bridge_received_at`) not written to target table. | Re-aligned query blocks to capture exact pipeline keys (`batch_id`). |

## Operational Results

### Compilation Metrics

| Model | Result |
|---|---|
| `clean_sensor_data` (Silver Layer) | 44,501 raw target rows processed into isolated tables in 0.69 seconds. |
| `hourly_metrics` (Gold Layer) | Materialized 5 highly accurate metric rows from raw historical frames in 0.37 seconds. |

### Data Lineage Map

```text
Supabase Data Instance (44,501 transactional entries)
           ↓
    [ dbt Source Mapping ]
           ↓
clean_sensor_data (Silver Dataset - cleansed & categorized)
           ↓
    [ dbt Reference Parsing ]
           ↓
hourly_metrics (Gold Dataset - hourly aggregated roll-ups)
```

### Performance Baseline Breakdown

| Metric Element | Evaluated Value |
|---|---:|
| Total Records Parsed | 44,501 rows |
| Silver Materialization Latency | 0.69s |
| Gold Aggregation Latency | 0.37s |
| Total dbt Cycle Footprint | 2.31s |

## Trade-offs Considered

### Positive Architectural Wins

- Version Control Analytics: Entire semantic transformation catalog sits as pure code within standard Git branches.
- Automated Lineage Graphs: Generating structural project maps outputs full visually navigable documentation DAGs.
- Highly Reproducible Runs: Bypassing global system drift guarantees identical database shapes across all development sandboxes.

### Negative Platform Overhead

- Jinja Syntax Overhead: Introduces a learning ramp for writing parameterizations inside query sheets.
- Environment Isolation Constraints: Requires distinct package injection loops (`--no-deps`) to cleanly pass host version locks.

## Medallion Layer Mapping Impact

| Medallion Layer | Target Storage Engine | Active Management Tool | Record Status |
|---|---|---|---|
| Bronze Layer | MongoDB Atlas Cluster | PyMongo / Raw Apache Kafka Router | 11,500+ unfiltered frames |
| Silver Layer | Supabase PostgreSQL Instance | Airflow Ingestion DAG / Custom C-Extensions | 44,501 validated structural records |
| Gold Layer | Supabase Analytics Instance | dbt-core v1.10 Engine Matrix | 5 highly aggregated analytics metrics |

## Resume Impact Statement

> "Implemented a dbt analytics layer processing 44,501 validated IoT sensor records, creating Silver (cleansed) and Gold (aggregated hourly metrics) database layers with full version control, automated catalog documentation, and explicit tracking data lineage workflows."

## References

- dbt Core v1.10 Release Engineering Notes
- Supabase PostgreSQL Connection Pooler Routing Spec
- Project Optimization Metrics Profile (16GB RAM / 8GB Disk Profile Framework)