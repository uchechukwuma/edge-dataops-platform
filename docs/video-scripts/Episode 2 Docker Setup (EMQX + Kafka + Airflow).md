# Episode 2: Docker Infrastructure

## Hook (30 seconds)
"One command to spin up EMQX, Kafka, and Airflow. Let me show you how."

## Why Docker Compose (2 minutes)
- Infrastructure as Code
- Reproducible for any machine
- Single `docker compose up -d` command

## The Services (5 minutes)

### EMQX (MQTT Broker)
- Enterprise-grade (BMW, Tesla use it)
- Built-in dashboard on port 18083
- WebSocket client for testing

### Kafka (KRaft Mode)
- No ZooKeeper (saves memory)
- 1-hour retention (disk limit)
- Port 9092

### Airflow (LocalExecutor)
- No Redis/Celery (saves resources)
- Web UI on port 8080
- Admin credentials: admin/admin

## Docker Compose Walkthrough (5 minutes)
- Show the YAML file
- Volume mounts (persistence)
- Health checks
- Logging limits

## Verification (2 minutes)
- `docker ps` shows 5 containers
- EMQX dashboard: http://localhost:18083
- Airflow UI: http://localhost:8080

## Disk Management (1 minute)
- data/kafka/ (500MB limit)
- data/postgres/ (Airflow metadata)
- data/emqx/ (configuration)

## What's Next (30 seconds)
"Episode 3: The C-extension — writing, compiling, and benchmarking 10M+ msg/sec."