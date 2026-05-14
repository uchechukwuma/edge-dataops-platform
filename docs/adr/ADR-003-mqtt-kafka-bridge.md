# ADR-003: MQTT to Kafka Bridge Architecture

**Date:** 2026-05-14
**Status:** Accepted

## Context
The pipeline needs to ingest data from IoT devices (MQTT) and distribute to downstream systems (Kafka). These protocols serve different purposes and should be decoupled.

## Decision
Implement a separate bridge service that:
- Consumes from EMQX (MQTT) on topic `sensors/telemetry`
- Produces to Kafka on topic `sensor-data`
- Runs as an independent Python process

## Performance Results
| Metric | Result |
|--------|--------|
| Messages processed | 44,000+ |
| Success rate | 100% |
| Failed messages | 0 |

## Architecture Benefits
- **Decoupling:** MQTT and Kafka can scale independently
- **Resilience:** Bridge can restart without data loss
- **Observability:** Clear separation of concerns

## Trade-offs
- **Positive:** Clean separation, easy to replace either broker
- **Negative:** Additional moving part (bridge process)
