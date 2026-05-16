# Episode 1: The Industrial-Inspired Vision

## Hook (30 seconds)
"What if you could build a data pipeline that validates 10 million sensor messages per second — without expensive hardware?"

## The Industrial-Inspired Approach (3 minutes)
- Not claiming to replace real PLCs
- Demonstrating architectural patterns that scale
- Edge validation, polyglot persistence, cloud offloading

## The Constraint That Drove Decisions (2 minutes)
- 8GB disk limit → Cloud offloading (MongoDB Atlas + Supabase)
- 16GB RAM → Kafka KRaft (no ZooKeeper)
- Python GIL → C-extension

## Architecture Overview (4 minutes)
- Edge (C-extension validation)
- Messaging (EMQX + Kafka)
- Orchestration (Airflow as Control Plane)
- Storage (Polyglot: MongoDB + PostgreSQL)
- Analytics (dbt + Streamlit)

## What Makes This Different (2 minutes)
- Not a bootcamp project
- Architecture Decision Records (ADRs)
- Production constraints respected
- Real hardware ready (lab sensors)

## Series Roadmap (1 minute)
- Episode 2: Docker infrastructure
- Episode 3: C-extension (10M msg/sec)
- Episode 4: MQTT → Kafka bridge
- Episode 5: Airflow Control Plane
- Episode 6: Cloud offloading (8GB constraint solved)
- Episode 7: dbt transformations
- Episode 8: Streamlit dashboard

## Call to Action (30 seconds)
"Subscribe to follow along as we build an industrial-inspired DataOps platform."