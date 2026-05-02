# ADR-001: Hybrid-Cloud Polyglot Persistence & Resource Optimization

**Date:** 2026-04-28 (Updated)
**Status:** Accepted

## Context
The project requires a full-lifecycle DataOps platform (Ingestion, Streaming, Orchestration, Storage) with constraints:
- 8GB free disk space (tight limit)
- 16GB RAM (adequate)
- Windows 11 + Docker Desktop (networking quirks)

## Decisions

### 1. MQTT Broker: EMQX (not Mosquitto)
**Initial choice:** Mosquitto 2.0 → **Failed** (config parsing errors, container crash loop)

**Final choice:** EMQX 5.0.26 

**Why EMQX:**
- Worked immediately with default config
- Built-in WebSocket dashboard on port 18083
- Enterprise adoption (BMW, Tesla, Volkswagen)
- No fragile config files required

### 2. Kafka: KRaft mode (no ZooKeeper)
Saves ~500MB disk and simplifies architecture

### 3. Storage Strategy (Polyglot)
| Layer | Technology | Location | Rationale |
|-------|------------|----------|-----------|
| Bronze (raw) | MongoDB Atlas | Cloud | 0GB local, schema-less |
| Silver/Gold | Supabase | Cloud | 0GB local, dbt target |
| Orchestration | Airflow LocalExecutor | Local | No Redis/Celery |

### 4. Windows Docker Networking Workaround
**Issue:** Temporary containers cannot reach localhost  
**Solution:** Use EMQX WebSocket dashboard for testing; host Python works

## Current Status 
- EMQX dashboard: http://localhost:18083 (admin/public)
- Airflow UI: http://localhost:8080 (admin/admin)
- Kafka: Running in KRaft mode
- Total disk usage: ~4.5GB (within 8GB limit)

## Future Improvements
- MQTT authentication
- Kafka SSL/TLS
- Terraform for cloud resources