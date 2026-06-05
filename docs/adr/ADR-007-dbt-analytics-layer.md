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
| **Version Control** | ✅ Native compilation logic in Git | ❌ Manual layout tracking |
| **Documentation** | ✅ Auto-generated schema catalogs | ❌ None |
| **Lineage Tracking** | ✅ Visual DAG dependencies | ❌ Manual map graphing |
| **Testing Hooks** | ✅ Built-in constraints parsing | ❌ Custom execution blocks |
| **Industry Standard**| ✅ High portability ecosystem | ⚠️ Legacy pattern fragmentation |

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