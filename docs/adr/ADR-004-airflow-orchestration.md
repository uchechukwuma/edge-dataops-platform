# ADR-004: Airflow for Pipeline Orchestration

**Date:** 2026-05-16
**Status:** Accepted
**Supersedes:** None

## Context

The pipeline currently has:
- Data ingestion (simulator → EMQX)
- Message bridging (EMQX → Kafka)
- High-performance validation (C-extension at 10M+ msg/sec)
- Message storage (Kafka with 1-hour retention)

**Missing:** Scheduled, reliable orchestration to:
1. Read from Kafka periodically
2. Aggregate and transform sensor data
3. Prepare data for cloud storage (Week 5)
4. Monitor pipeline health and retry on failures
5. Provide visibility into data processing

**Constraints:**
- Must run on 16GB RAM / 8GB disk
- Must integrate with existing Docker Compose stack
- Should follow industry standards for data engineering


## Decision

**Use Apache Airflow 2.7.2 with LocalExecutor for orchestration.**

### Why Airflow?

| Factor | Airflow | Alternatives |
|--------|---------|--------------|
| Industry adoption |  Standard for data engineering | Dagster (newer), Prefect (less common) |
| Job market relevance |  Required in 40%+ DE job postings | Limited for alternatives |
| Learning curve | Moderate | Steep for some |
| Resource overhead | ~500MB RAM (LocalExecutor) | Similar |
| UI/monitoring |  Rich web interface | Varies |
| Retry/alerting |  Built-in | Varies |
| Docker integration |  Native support | Yes |

## DAG Structure

```text
┌──────────────────────┐     ┌──────────────────────┐     ┌──────────────────────┐
│ consume_from_kafka   │───▶│ aggregate_data       │───▶ │ prepare_for_cloud    │
└──────────────────────┘     └──────────────────────┘     └──────────────────────┘
```


| Task | Function | Input | Output |
|------|----------|-------|--------|
| `consume_from_kafka` | Read messages from Kafka | Kafka topic `sensor-data` | Statistics dictionary |
| `aggregate_data` | Count by sensor type | Statistics | Summary JSON file |
| `prepare_for_cloud` | Stage for Week 5 | Summary file | Ready flag |

### Configuration Details

```python
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sensor_data_pipeline',
    default_args=default_args,
    description='Process sensor data from Kafka',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['sensors', 'kafka', 'industry40'],
)
```

**Executor Choice: LocalExecutor**
## Why LocalExecutor:
- Fits within 16GB RAM constraint
- No additional containers (Redis, Celery workers)
- Sufficient for portfolio-scale pipeline
- Can upgrade to Celery later if needed

## Implementation Details
### Kafka Connectivity Fix
Problem: Airflow containers couldn't reach Kafka using localhost:9092
Root cause: Kafka's advertised.listeners was set to localhost:9092, which inside a Docker container points to the container itself, not the host.

Solution: Changed advertised.listeners to use Docker service name:
# docker-compose.yml
kafka:
  environment:
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka_broker:9092

### Verification:
```bash
docker exec airflow_webserver python -c "
from kafka import KafkaConsumer
consumer = KafkaConsumer('sensor-data', bootstrap_servers='kafka_broker:9092')
print('>> Connected')
"
```
## Dependencies Installation
Airflow containers required kafka-python package:
```bash
docker exec -u root airflow_webserver pip install kafka-python
docker exec -u root airflow_scheduler pip install kafka-python
```

## DAG File Location
```yaml
# docker-compose.yml
airflow-webserver:
  volumes:
    - ./dags:/opt/airflow/dags  # Mount local dags folder
```

## Performance Results
Metric:	Result
Messages processed:	62,000+
Successful DAG runs:	10+
Task success rate:	100%
Kafka connection stability:	Consistent
Average execution time:	~5 seconds
Schedule interval:	Every 5 minutes

## Sample Log Output

```text
[INFO] >> Connected to Kafka at kafka_broker:9092
[INFO] Consuming from topic: sensor-data
[INFO] Processed 1000 messages...
[INFO] Processed 2000 messages...
...
[INFO] >> Final Stats - Total: 62000 | Validated: 62000 | Failed: 0
[INFO] Marking task as SUCCESS
```

## Challenges & Resolutions

| Challenge | Root Cause | Resolution |
|---|---|---|
| NoBrokersAvailable | `advertised.listeners` set to `localhost` | Changed to `kafka_broker:9092` |
| `ModuleNotFoundError: kafka` | Missing package in Airflow containers | Ran `pip install kafka-python` in both containers |
| DAG not appearing in UI | Volume mount not configured | Added `./dags:/opt/airflow/dags` |
| `localhost` connection errors in logs | Kafka metadata returning `localhost` | Verified fix by checking logs |

## Trade-offs

### Positive 

| Benefit | Details |
|---|---|
| Industry-standard orchestration | Apache Airflow is widely used in production data platforms. |
| Rich web UI for monitoring | Strong visibility in the UI: [http://localhost:8080](http://localhost:8080/) |
| Built-in retry and alerting | Makes failures easier to recover from automatically. |
| Clear task dependencies visualization | DAG structure is easy to understand and debug. |
| XCom for data passing between tasks | Useful for lightweight inter-task communication. |
| Schedule flexibility | Supports cron expressions and custom schedules. |
| Easy to add new tasks | DAGs can be extended without redesigning the whole pipeline. |

### Negative 

| Drawback | Details |
|---|---|
| Additional container overhead | Adds roughly 500MB RAM or more in local setups. |
| Learning curve for DAG syntax | Requires familiarity with Python-based workflow definitions. |
| LocalExecutor scaling limits | Does not scale horizontally well. |
| Python environment requirement | Dependencies must be installed inside containers. |
| Initial setup complexity | Networking, mounts, and dependencies can be tricky at first. |

### Alternatives Considered

| Tool | Throughput | Complexity | Industry Adoption | Verdict |
|---|---|---|---|---|
| Airflow | Medium | Medium | Very High |  Selected |
| Cron + bash scripts | Low | Low | Low |  No monitoring/retries |
| Prefect | Medium | Medium | Low |  Less job market value |
| Dagster | Medium | High | Very Low |  Too new |
| Temporal | High | High | Low |  Overkill for this use |

### Future Considerations

| Improvement | When | Effort |
|---|---|---|
| CeleryExecutor for parallel tasks | If scale > 10k msg/min | Medium |
| Alerting (Slack/email) | Week 7-8 | Low |
| Sensors for Kafka topic | If real-time needed | Low |
| KubernetesExecutor | Cloud deployment | High |

### Resume Impact Statement

> *"Orchestrated a streaming sensor pipeline using Apache Airflow, processing 62,000+ messages with automated scheduling every 5 minutes, achieving 100% task success rate and zero data loss across 10+ DAG runs."*

### Related ADRs

| ADR | Relationship |
|---|---|
| [ADR-001](https://./ADR-001-hybrid-cloud-polyglot.md) | Infrastructure foundation |
| [ADR-002](https://./ADR-002-c-extension-validation.md) | Validation layer |
| [ADR-003](https://./ADR-003-mqtt-kafka-bridge.md) | Streaming backbone |
| [ADR-004] | Orchestration layer (this document) |

### References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow LocalExecutor](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/local.html)
- [Kafka advertised.listeners explanation](https://rmoff.net/2018/08/02/kafka-listeners-explained/)