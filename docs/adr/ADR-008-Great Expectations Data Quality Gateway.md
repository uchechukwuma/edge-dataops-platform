# ADR-008: Great Expectations Data Quality Gateway

- **Date:** 2026-06-18
- **Status:** Accepted
- **Supersedes:** None

## Context

The platform already has three logical layers:

- **Bronze:** raw event persistence in MongoDB.
- **Silver:** cleaned and normalized data in Supabase.
- **Gold:** analytical transformations in dbt.

After validating 181,000+ records in the Silver layer, the team needed a reliable quality gate to prevent invalid data from reaching dbt models and downstream dashboards.

The main problems were:

- No automated validation between Silver and Gold.
- Risk of corrupted, incomplete, or malformed data contaminating analytical outputs.
- Need for auditable validation results for debugging, observability, and operational traceability.
- Need for a lightweight gateway that can run inside the existing Airflow orchestration flow.

## Decision

Implement **Great Expectations (GX) v1.18.1** as the data quality gateway between the Silver and Gold layers.

GX will execute as a dedicated validation step in Airflow. If validation passes, dbt transformations proceed. If validation fails, the pipeline stops immediately and downstream processing is blocked.

## Why Great Expectations?

| Factor | Great Expectations | Custom Scripts |
|---|---|---|
| Built-in expectations | Rich set of reusable checks | Every check must be coded manually |
| Validation reporting | Automatic Validation Results and Data Docs | Must be implemented separately |
| SQL execution | Supports SQL-backed validation workflows | Manual database logic required |
| Airflow integration | Works naturally as a Python validation step | Requires custom orchestration glue |
| Auditability | Strong traceability of checks and outcomes | Harder to standardize and review |

## Implementation Details

### Architecture

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                        GREAT EXPECTATIONS GATEWAY                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Airflow Task: run_data_quality_checks                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  1. SQLAlchemy pushdown validation                                   │   │
│  │  2. Apply semantic expectations to Silver-layer records              │   │
│  │  3. If passed → set gx_passed = True → trigger dbt                   │   │
│  │  4. If failed → raise error → block dbt execution                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Environment Setup

```python
# scripts/gx_full_pipeline.py
import os
from dotenv import load_dotenv
import great_expectations as gx

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")

context = gx.get_context(project_root_dir="gx/")
datasource = context.data_sources.add_postgres(
    name="supabase_silver",
    connection_string=SUPABASE_URL,
)
```

### Validation Strategy

The expectation suite enforces structural, semantic, and boundary-level checks on Silver-layer data before transformation.

| Expectation | Purpose |
|---|---|
| `expect_table_row_count_to_be_between` | Validate record volume and detect floods or truncation |
| `expect_column_to_exist` | Verify schema integrity |
| `expect_column_values_to_match_regex` | Enforce sensor or entity identifier format |
| `expect_column_values_to_be_between` | Detect out-of-range values |
| `expect_column_values_to_be_in_set` | Validate allowed units or categorical values |
| `expect_column_values_to_not_be_null` | Prevent incomplete records from entering dbt |
| `expect_column_values_to_be_unique` | Reduce duplicate-driven distortion |
| `expect_column_values_to_match_regex` | Check formatting of checksum or signature fields |

## Airflow Integration

```python
# sensor_data_pipeline_production.py

def run_data_quality_checks(**context):
    """Run Great Expectations validation on the Silver layer."""
    context_gx = gx.get_context(project_root_dir="/opt/airflow/gx/")
    datasource = context_gx.data_sources.get("supabase_silver")
    data_asset = datasource.get_asset("silver_sensor_data")
    suite = context_gx.suites.get("silver_sensor_data_suite")

    validator = context_gx.get_validator(
        batch_request=data_asset.build_batch_request(),
        expectation_suite=suite,
    )

    results = validator.validate()

    if not results.success:
        raise ValueError("Data quality checks failed - stopping pipeline")

    return True
```

### DAG Dependency

```text
consume_from_kafka
        ↓
write_to_mongodb (Bronze)
        ↓
write_to_supabase (Silver)
        ↓
run_data_quality_checks (GX Gateway)
        ↓
    ┌───┴───┐
    ↓       ↓
  PASS     FAIL
    ↓       ↓
 summary   pipeline stops
 task
```

## Operational Results

### Quality Metrics

| Metric | Value |
|---|---|
| Total records validated | 181,000+ |
| Active expectations | 12 |
| Validation status | Passed |
| Validation coverage | 100% of defined expectations |
| SQL pushdown compute | Enabled |

### Runtime Characteristics

| Metric | Value |
|---|---|
| Validation time | < 5 seconds |
| Memory usage | ~50 MB |
| Airflow task status | Successful and fail-fast |

## Safety and Reliability Mapping

The gateway follows a safety-barrier mindset inspired by industrial communication systems: validate before trust, and stop the pipeline on inconsistency.

| Risk | Mitigation |
|---|---|
| Corruption | Range checks, regex checks, and checksum validation |
| Repetition | Deduplication in the Silver layer |
| Masquerading | Identifier and unit validation |
| Delay | Airflow monitoring and task-level visibility |
| Loss | Kafka retention and replay |
| Reordering | Kafka offsets and ordered ingestion semantics |
| Overflow | Row-count thresholds and validation limits |

## Trade-offs Considered

### Benefits

- Prevents bad data from reaching dbt.
- Keeps validation close to the orchestration layer.
- Produces auditable validation artifacts.
- Avoids custom validation framework maintenance.

### Costs

- Adds a dependency to the platform.
- Requires expectation maintenance as schemas evolve.
- Introduces a modest runtime step before transformation.

## Consequences

### Positive

- Higher confidence in Gold-layer outputs.
- Easier debugging when data anomalies appear.
- Better separation between ingestion, validation, and analytics.
- Stronger observability for the full pipeline.

### Negative

- Slightly longer end-to-end runtime.
- More operational discipline required to maintain expectations.
- Additional GX setup and documentation work.

## Resume Impact Statement

Designed and implemented a production-grade data quality gateway for an industrial IoT pipeline processing 181,000+ sensor records, using Great Expectations with SQLAlchemy pushdown validation to enforce semantic constraints, block invalid data before dbt transformations, and improve the reliability and auditability of downstream analytics.

## References

- Great Expectations documentation
- Airflow integration documentation
- Data Docs and Validation Results documentation
- ADR-005: Cloud Integration
- ADR-007: dbt Analytics Layer