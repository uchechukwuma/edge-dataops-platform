# ⚡ Edge DataOps Platform

An enterprise-grade, resource-optimized industrial IoT telemetry pipeline engineered with C-optimized processing extensions, high-throughput message brokers, distributed streaming backbones, and cloud-native polyglot storage layers.

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![EMQX](https://img.shields.io/badge/EMQX-47B8D0?style=for-the-badge&logo=mqtt&logoColor=white)](https://www.emqx.com)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com)

---

##  What Makes This Unique

Unlike standard prototype pipelines, this platform is explicitly architected around edge computing physical constraints:

* **⚡ Low-Latency C-Extension Validation:** Implemented a custom C-library binding layer to achieve **15,000+ messages/second** data checksum verification, executing 7.5x faster than native Python loops.
* **📐 High-Efficiency KRaft Brokerage:** Executed Apache Kafka in native KRaft mode, removing the overhead dependency of Apache ZooKeeper and reclaiming **500MB** of localized container memory footprint.
* **🏗️ Managed Polyglot Persistence Tier:** Decoupled storage workloads into a hybrid topology using MongoDB Atlas for raw transactional Bronze event logs and Supabase PostgreSQL for refined, analytics-ready Silver/Gold metrics.
* **💰 Free-Tier Optimized Storage:** Implemented strategic 1-in-10 sampling (90% storage reduction) and 24-hour auto-pruning, achieving **12+ months of free tier sustainability** at 90+ messages/second.
* **📝 Production Architecture Decision Records (ADRs):** Every structural tool choice, networking path, and schema definition is fully mapped out with rigorous trade-off logs.

## 🏗️ Updated Architecture Diagram

```mermaid
flowchart TB
    subgraph Edge["⚡ EDGE LAYER (Local - 16GB/8GB Optimization)"]
        Sensor[Python Simulator<br/>+ C-Extension Validation<br/>15k+ msg/sec]
        EMQX[EMQX Message Broker<br/>MQTT Port :1883]
    end

    subgraph Backbone["🔄 STREAM BACKBONE (Local Docker Ecosystem)"]
        Kafka[Kafka KRaft Core<br/>Port 9092<br/>1hr Log Retention<br/>CLUSTER_ID Initialized]
        Airflow[Airflow Orchestrator<br/>Port 8080<br/>confluent_kafka Consumer<br/>2-Min Execution Loops]
    end

    subgraph Sampling["🎯 SAMPLING & PRUNING LAYER"]
        Decision{1-in-10 Sampling}
        Prune[24-Hour Auto-Prune]
    end

    subgraph Cloud["☁️ CLOUD STORAGE TIER (Free Tier Optimized)"]
        MongoDB[(MongoDB Atlas<br/>Bronze Layer - Sampled Raw Logs<br/>1-in-10 + 24h Retention<br/>~5MB/month)]
        Supabase[(Supabase PostgreSQL<br/>Silver/Gold - Clean Warehousing<br/>100% Validated Records<br/>~1MB/month)]
    end

    subgraph Analytics["📊 ANALYTICS & PRESENTATION LAYER"]
        dbt[dbt Transformations<br/>SQL Analytical Mart Models]
        Streamlit[Streamlit BI Dashboard<br/>Port 8501]
    end

    Sensor -->|MQTT Protocol| EMQX
    EMQX -->|Custom Broker Bridge| Kafka
    Kafka -->|Log Consumed By| Airflow
    Airflow -->|Raw Batch| Decision
    Decision -->|90% discard| Prune
    Decision -->|10% keep| MongoDB
    Airflow -->|Validated Only| Supabase
    Supabase -->|Transform T-Layer Lineage| dbt
    Supabase -->|Direct Metric Ingestion Query| Streamlit
```

- **For a detailed breakdown of architectural trade-offs, research goals, and project history, see the Full Project Report.**

## Project Structure

```text
edge-dataops-platform/
├── data/                      # Persistent Local Volume Mount Storage
│   ├── kafka/                 # Kafka broker cluster segment logs (500MB limit)
│   ├── postgres/              # Local Airflow state metadata engine database
│   └── emqx/                  # EMQX configuration states + retained telemetry
├── logs/                      # Automated rolling system log rotation outputs
│   └── emqx/
├── c_library/                 # C-Extension Processing Engine
│   ├── validator.c            # Native C low-latency rolling checksum algorithm
│   ├── setup.py               # Python C-types build integration deployment script
│   └── test_validator.py      # High-speed data throughput benchmarking suite
├── dags/                      # Airflow Orchestration Execution DAG Maps
├── dbt_models/                # dbt analytical transform layer (Silver → Gold)
│   ├── dbt_project.yml        # dbt core configuration schema map
│   └── models/                # Modular analytics mart queries
├── ingestor/                  # Industrial IoT sensor simulator configuration
│   └── sensor_simulator.py
├── bridge/                    # Custom cross-protocol sync handler
│   └── mqtt_to_kafka.py       # High-performance MQTT-to-Kafka consumer engine
├── tests/                     # Automated data validation mapping
│   └── expect_suite.py        # Great Expectations schema assert suites
├── docs/                      # Technical System Records
│   ├── adr/                   # Formal Architecture Decision Records
│   │   ├── ADR-001-hybrid-cloud-polyglot.md
│   │   ├── ADR-002-c-extension-validation.md
│   │   ├── ADR-003-mqtt-kafka-bridge.md
│   │   ├── ADR-004-airflow-orchestration.md
│   │   └── ADR-005-cloud-integration.md
│   └── PROJECT_REPORT.md      # Comprehensive deep-dive master research paper
├── docker-compose.yml         # Containerized automated system infrastructure orchestrator
└── README.md                  # System Blueprint Manifest Document
```


## Quick Start

### Prerequisites
- Docker Desktop (16GB RAM recommended, 8GB minimum)
- 8GB free system disk allocation space
- Local Python 3.9+ runtime environment

### Booting the Infrastructure Stack

```bash
# Clone repository
git clone https://github.com/uchechukwuma/edge-dataops-platform
cd edge-dataops-platform

# Start all services
docker compose up -d

# Verify containers are running
docker ps
```


## Active Microservice Endpoints

| Service | URL | Credentials |
|---|---|---|
| EMQX Dashboard | [http://localhost:18083](http://localhost:18083/) | admin / public |
| Airflow UI | [http://localhost:8080](http://localhost:8080/) | admin / admin |
| MQTT Broker | localhost:1883 | No auth (dev) |
| Kafka Broker | localhost:9092 | No auth |

## Stop Everything
To gracefully spin down and release localized container networking memory allocations, run:
```Bash
docker compose down
```

## Performance Benchmarks

| Test | Expected | Actual | Status |
|:---|:---:|:---:|:---:|
| C-extension throughput | 15,000 msg/sec | 15,000+ msg/sec | OK |
| Validation latency | <0.1ms | ~0.07ms | OK |
| Bridge success rate | 100% | 0% loss | OK |
| MongoDB documents | N/A | 11,500+ | OK |
| Supabase records | N/A | 1,001+ | OK |
| Free tier projection | N/A | 12+ months | OK |

## Architecture Decision Records (ADRs)

| ADR | Decision | Impact |
|-----|----------|--------|
| [ADR-001](docs/adr/ADR-001-hybrid-cloud-polyglot.md) | EMQX over Mosquitto, Kafka KRaft, cloud offloading | Saved 500MB RAM |
| [ADR-002](docs/adr/ADR-002-c-extension-validation.md) | C-extension over pure Python | 15k+ msg/sec (7.5x faster) |
| [ADR-003](docs/adr/ADR-003-mqtt-kafka-bridge.md) | MQTT to Kafka bridge architecture | 44k+ msgs, 0% loss |
| [ADR-004](docs/adr/ADR-004-airflow-orchestration.md) | Airflow DAG orchestration | 62k+ msgs, every 5 min |
| [ADR-005](docs/adr/ADR-005-cloud-integration.md) | Cloud storage with sampling | 12+ months free tier |
| [ADR-006](docs/adr/ADR-006-kafka-consumer-stabilization.md) | KRaft + confluent_kafka fix | Resolved consumer deadlock |

## Roadmap

| Week | Focus | Status | Notes |
|---:|---|---|---|
| 1 | Docker infrastructure (EMQX + Kafka + Airflow) |  Complete | |
| 2 | C-extension compilation & benchmark |  Complete | 15k+ msg/sec |
| 3 | MQTT → Kafka bridge |  Complete | 44k+ msgs, 0% loss |
| 4 | Airflow DAG #1 (Bronze → Silver) |  Complete | 62k+ msgs processed |
| 5 | Cloud integration (Supabase + MongoDB Atlas) |  Complete | 11,500+ docs, 1,001+ records |
| 6 | dbt transformations (Silver → Gold) | ⏳ Pending | |
| 7 | Great Expectations tests | ⏳ Pending | |
| 8 | Streamlit dashboard + demo video | ⏳ Pending | |

## Disk Space Management

| Component | Location | Max Size | Auto-cleanup |
|---|---|---:|---|
| Kafka logs | `data/kafka/` | 500 MB | 1 hour retention |
| Postgres (Airflow) | `data/postgres/` | 100 MB | Manual |
| EMQX data | `data/emqx/` | 50 MB | Manual |
| Docker images | Docker store | 3 GB | `docker system prune` |

*Total expected: ~4.5 GB (well within the strict 8 GB edge limitation host pool).*

---

## Quick Test: Validating MQTT Ingestion Flow

1. Access the web console interface at **`http://localhost:18083`** using `admin` / `public`.
2. Select the **WebSocket Client** widget panel, and execute a connection state open.
3. Establish a standard topic subscriber line targeting the channel: **`test`**.
4. Generate a test message payload targeting the channel with this structured block:
   ```json
   {"sensor": "temp", "value": 23.5}
    ```
5. Confirm instant event delivery visualization inside your tracking dashboard logs.

## Troubleshooting & Known Resolutions

### Issue: Kafka Consumer Connection Hang (Resolved)

**Symptom:** Airflow consumer connects but never receives messages

**Root Cause:** KRaft cluster uninitialized + advertised listener misconfiguration

**Resolution:**
1. Added `CLUSTER_ID` environment variable for auto-formatting
2. Configured split listeners (INTERNAL/EXTERNAL) for Docker networking
3. Switched from `kafka-python` to `confluent_kafka` (C-extension)

### Issue: Supabase DNS Resolution (Resolved)

**Symptom:** `could not translate host name to address`

**Root Cause:** Direct connection uses IPv6; Docker/WSL2 inconsistent IPv6 support

**Resolution:** Switched to Transaction Pooler endpoint (IPv4-compatible)

### Issue: MongoDB Free Tier Capacity (Mitigated)

**Solution:** Implemented 1-in-10 sampling + 24-hour auto-prune
- **Before:** 11,500 docs/day (~50MB)
- **After:** ~1,150 docs/day (~5MB)
- **Result:** 12+ months of free tier sustainability

## License
This platform is deployed under the terms of the MIT License.

## Connect
* linkedin.com/in/uchechukwu-obi 
* https://github.com/uchechukwuma/edge-dataops-platform