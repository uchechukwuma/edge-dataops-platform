# Edge DataOps Platform & Functional Safety Gateway

An industrial IoT telemetry platform that combines C-optimized validation, message brokering, stream processing, cloud-native storage, and automated data quality checks into a single end-to-end pipeline.

The system is designed around a **defensive safety communication layer inspired by EN 50159 principles** for untrusted transmission environments. It treats edge-to-cloud communication as a **gray channel** and applies validation, sampling, orchestration, and fail-fast checks before data reaches analytics or dashboard layers. EN 50159 is a safety-related communication standard that describes safety communication over transmission systems, and this project borrows its safety-layer mindset without claiming formal certification. [web:99][web:129]

---

## Overview

This repository demonstrates a production-style data platform with the following goals:

- Validate telemetry early at the edge.
- Move events reliably through MQTT and Kafka.
- Store raw and curated data in separate persistence layers.
- Apply automated data quality checks before dbt transformations.
- Keep the architecture lightweight enough to run in a constrained local environment.
- Document design decisions through Architecture Decision Records (ADRs).

---

## Core Features

- **C-optimized validation layer** for fast checksum and payload screening.
- **MQTT ingestion** through EMQX for edge telemetry transport.
- **Kafka event backbone** for buffering and stream handoff.
- **Airflow orchestration** for scheduled pipeline execution.
- **Great Expectations quality gateway** for fail-fast validation between Silver and Gold layers.
- **dbt transformations** for analytical modeling.
- **Polyglot persistence** using MongoDB Atlas and Supabase PostgreSQL.
- **Resource-aware design** with sampling and pruning to stay within free-tier and local machine limits.
- **Formal documentation** through ADRs, project report, and functional safety case.

---

## Architecture

```text
Sensor Simulator
   ↓
C Validation Layer
   ↓
EMQX MQTT Broker
   ↓
Apache Kafka
   ↓
Airflow Orchestrator
   ↓
Great Expectations Gateway
   ↓
Supabase PostgreSQL (Silver)
   ↓
dbt Transformations
   ↓
Gold Analytics Layer
   ↓
Streamlit Dashboard
```

### Storage Layers

- **Bronze:** MongoDB Atlas for sampled raw telemetry logs.
- **Silver:** Supabase PostgreSQL for validated and normalized data.
- **Gold:** dbt models for curated analytical outputs.

---

## Technology Stack

| Layer | Tools |
|---|---|
| Edge validation | C, Python |
| Messaging | EMQX, MQTT, Kafka |
| Orchestration | Airflow |
| Data quality | Great Expectations |
| Transformation | dbt |
| Storage | MongoDB Atlas, Supabase PostgreSQL |
| Presentation | Streamlit |
| DevOps | Docker, Docker Compose |

---

## Project Structure

```plaintext
edge-dataops-platform/
├── data/
├── logs/
├── c_library/
├── dags/
├── dbt_models/
├── ingestor/
├── bridge/
├── scripts/
├── docs/
│   ├── adr/
│   ├── PROJECT_REPORT.md
│   └── FUNCTIONAL_SAFETY_CASE.md
├── docker-compose.yml
└── README.md
```

---

## Key Results

| Metric | Result |
|---|---|
| C-extension throughput | 15,000+ messages/sec |
| Data quality checks | 12 rules enforced |
| Validated records | 181,000+ |
| dbt runtime | 44,501 records in 2.31s |
| Memory overhead | ~50 MB |
| Message loss | 0% in test blocks |
| Free-tier sustainability | 12+ months target |

---

## Safety Approach

The platform applies a safety-inspired communication mindset:

- Check payload integrity before trust.
- Reject malformed or suspicious records early.
- Use a fail-fast gate before analytics.
- Keep validation results auditable.
- Separate raw, validated, and modeled data clearly.

This is a conceptual safety design inspired by EN 50159-style communication barriers, not a formal certification claim. [web:99][web:129]

---

## Architecture Decision Records

Major implementation choices are documented in the ADR series:

- ADR-001: Hybrid Cloud Polyglot Decoupling Strategy
- ADR-002: Low-Latency C-Extension Verification
- ADR-003: MQTT-to-Kafka Cross-Protocol Bridge
- ADR-004: Airflow Orchestration
- ADR-005: Free-Tier Resource Constraints via Sampling
- ADR-006: Kafka Consumer Stabilization
- ADR-007: dbt Analytics Layer
- ADR-008: Great Expectations Quality Gateway

---

## Quick Start

### Prerequisites

- Docker Desktop.
- Local Python 3.9+.
- Enough disk space for containers and volumes.

### Run the stack

```bash
git clone https://github.com/uchechukwuma/edge-dataops-platform
cd edge-dataops-platform
docker compose up -d
docker ps
```

### Stop the stack

```bash
docker compose down
```

---

## Local Endpoints

| Service | URL | Notes |
|---|---|---|
| EMQX Dashboard | http://localhost:18083 | Broker UI |
| Airflow UI | http://localhost:8080 | Pipeline orchestration |
| MQTT Broker | localhost:1883 | Development mode |
| Kafka Broker | localhost:9092 | Event backbone |
| Streamlit App | http://localhost:8501 | Dashboard, if enabled |

---

## Validation Workflow

The pipeline is designed so that data only reaches Gold after passing quality checks:

1. Sensor data is generated or ingested.
2. Payload integrity is verified in the validation layer.
3. Raw events are passed through the broker backbone.
4. Data is written into Bronze and Silver layers.
5. Great Expectations validates Silver-layer records.
6. Failed checks stop downstream dbt execution.
7. Passing records continue to Gold and analytics.

---

## Documentation

This repository includes the following deeper documents:

- `docs/PROJECT_REPORT.md` — full technical project report.
- `docs/FUNCTIONAL_SAFETY_CASE.md` — safety-oriented design and hazard discussion.
- `docs/adr/` — architecture decision records for each major design choice.

---

## Why This Project Matters

This project is useful because it shows:

- Systems thinking.
- End-to-end data engineering design.
- Practical use of data quality gates.
- Ability to work with edge, stream, and warehouse layers.
- Comfort with documentation and engineering trade-offs.

It is especially relevant for data engineering, industrial IoT, embedded-data, and smart-building roles.

---

## Resume Summary

Designed and implemented an industrial IoT Edge DataOps platform with C-optimized telemetry validation, MQTT and Kafka streaming, Airflow orchestration, Great Expectations quality gating, and dbt analytics, processing 181,000+ records with fail-fast validation and resource-aware deployment.

---

## License

MIT License.

---

## Contact

- GitHub: `github.com/uchechukwuma/edge-dataops-platform`
- LinkedIn: `linkedin.com/in/uchechukwu-obi`