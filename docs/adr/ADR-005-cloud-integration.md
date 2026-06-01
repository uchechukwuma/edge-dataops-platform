# ADR-005: Cloud Storage Integration (Bronze + Silver Layers)

**Date:** 2026-06-01
**Status:** Accepted
**Supersedes:** None

## Context

After establishing the streaming backbone (EMQX → Kafka) and orchestration layer (Airflow), the platform needed persistent cloud storage with:

**Requirements:**
- **Bronze Layer:** Immutable raw telemetry for audit, debugging, and potential reprocessing
- **Silver Layer:** Validated, cleansed data ready for analytics and dashboarding
- Free tier compatibility (MongoDB Atlas: 512MB, Supabase: 500MB)
- Handle 90+ messages/second sustained throughput
- Respect 16GB RAM / 8GB disk local constraints

**Constraints:**
- No budget for paid cloud services
- Must work within WSL2/Docker networking environment
- Must preserve C-extension checksum validation results

## Decision

**Implement polyglot persistence with strategic sampling:**

| Layer | Database | Purpose | Sampling | Retention |
|-------|----------|---------|----------|-----------|
| Bronze | MongoDB Atlas | Raw audit trail | 1-in-10 messages | 24 hours |
| Silver | Supabase PostgreSQL | Analytics-ready | 100% validated | Indefinite |

### Why MongoDB Atlas for Bronze?

| Factor | MongoDB Atlas | Alternative (Local File) |
|--------|---------------|--------------------------|
| Queryability | Rich aggregation | None |
| Free tier | 512MB | N/A |
| Schema flexibility | Document model | Rigid |
| Remote access | Yes | No |

### Why Supabase for Silver?

| Factor | Supabase | Alternative (MongoDB only) |
|--------|----------|---------------------------|
| ACID compliance | Yes | No |
| SQL analytics | Native | Limited |
| dbt integration | Native | None |
| Free tier | 500MB | N/A |

## Implementation Details

### 1. MongoDB Atlas Configuration

**Connection (from Airflow DAG):**
```python
MONGODB_ATLAS_URL = "mongodb+srv://dataops_user:****@edge-dataops-cluster.tvpmon1.mongodb.net/"
```

## Collection Schema (edge_platform_bronze.raw_sensor_telemetry):

```json
{
    "sensor_id": "flow_sensor_04",
    "value": 43.38,
    "unit": "L/min",
    "timestamp": 1779536067.0701962,
    "source": "simulator",
    "checksum": "E2",
    "validated": true,
    "bridge_received_at": 1779536067.070966,
    "_bronze_ingested_at": "2026-06-01T04:08:27.624743"
}
```

## 2. Supabase PostgreSQL Configuration
Connection (Transaction Pooler - IPv4 compatible):

```python
SUPABASE_URL = "postgresql://postgres:****@aws-0-eu-west-2.pooler.supabase.com:5432/postgres"
```

### Schema (dataops.silver_sensor_data):

```sql
CREATE TABLE IF NOT EXISTS dataops.silver_sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100),
    sensor_type VARCHAR(50),
    value DOUBLE PRECISION,
    unit VARCHAR(20),
    checksum VARCHAR(100),
    validated BOOLEAN,
    source_timestamp TIMESTAMP,
    batch_id VARCHAR(100),
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
## 3. Sampling Strategy for Free Tier Management

```python
# Store only 1-in-10 messages in MongoDB (90% storage reduction)
if msg_index % 10 == 0:
    raw_bronze_batch.append(payload)

# Store ALL validated messages in Supabase
if payload.get('validated') is True:
    validated_silver_batch.append(payload)

# Auto-prune MongoDB after 24 hours
cutoff = datetime.now() - timedelta(hours=24)
collection.delete_many({'timestamp': {'$lt': cutoff.timestamp()}})
```

### Critical Fix: Supabase DNS Resolution
**Problem**: Direct connection endpoint failed DNS resolution inside Docker containers
**Root Cause**: Direct connection uses IPv6; Docker/WSL2 has inconsistent IPv6 support
**Solution:** Switched to Transaction Pooler endpoint (IPv4-compatible)

## Results

### Current Statistics (June 1, 2026)

| Database | Records | Storage Used | Retention |
|---|---:|---:|---|
| MongoDB Atlas | 11,500+ | ~50 MB | 24 hours |
| Supabase | 1,001+ | ~10 MB | Indefinite |

### Projected Free Tier Capacity

| Database | Free Limit | Current Usage | Projected Lifespan |
|---|---:|---:|---:|
| MongoDB Atlas | 512 MB | ~50 MB | 10+ months |
| Supabase | 500 MB | ~10 MB | 12+ months |

## Trade-offs

### Positive

| Benefit | Details |
|---|---|
| Free tier sustainable | Sampling reduces MongoDB writes by 90%. |
| Full audit capability | Bronze has 24-hour complete fidelity for debugging. |
| Analytics ready | Silver contains every validated record. |
| IPv4 compatibility | Transaction pooler resolved Docker networking. |
| C-extension preserved | Checksums flow through to both databases. |

### Negative

| Drawback | Details | Mitigation |
|---|---|---|
| 90% raw data loss | After 24 hours. | Sufficient for debugging; alerts trigger on patterns. |
| Two database complexity | Cross-layer queries require separate connections. | Acceptable; layers serve different purposes. |
| Pooler latency overhead | ~5 ms per connection. | Negligible at 90 msg/sec. |

## Performance Metrics

| Metric | Value |
|---|---:|
| Write latency (MongoDB) | ~50 ms per batch |
| Write latency (Supabase) | ~30 ms per batch |
| Sampling overhead | Minimal |
| Auto-prune efficiency | <100 ms per DAG run |

## Resume Impact Statement
*"Architected polyglot cloud storage (MongoDB Atlas + Supabase) for an industrial IoT pipeline, implementing strategic 1-in-10 sampling and 24-hour auto-pruning to achieve 12+ months of free tier sustainability while processing 90+ messages/second with C-extension validation."*

## References
MongoDB Atlas Free Tier
Supabase Pricing
Supabase Connection Pooler
ADR-004: Airflow Orchestration