"""
Sensor Data Pipeline DAG - PRODUCTION GRADE with Confluent Kafka
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import logging
import os

# Import cloud libraries
try:
    import psycopg2
    from pymongo import MongoClient
    from confluent_kafka import Consumer, KafkaError
    CLOUD_AVAILABLE = True
except ImportError as e:
    CLOUD_AVAILABLE = False
    logging.warning(f"Cloud libraries not available: {e}")

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

def consume_from_kafka(**context):
    """Consume messages from Kafka using confluent_kafka"""
    
    KAFKA_BROKER = "kafka_broker:9092"
    KAFKA_TOPIC = "sensor-data"
    messages = []
    
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'airflow_production_confluent_v3',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 10000,
        'max.poll.interval.ms': 300000,
    }
    
    try:
        logger.info(f"Connecting to Kafka at {KAFKA_BROKER}")
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
        
        # Poll for messages
        max_messages = 500
        timeout_seconds = 5
        
        while len(messages) < max_messages:
            msg = consumer.poll(timeout=timeout_seconds)
            
            if msg is None:
                logger.info(f"No more messages after {len(messages)} consumed")
                break
                
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            # Successfully got a message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                messages.append(data)
                
                if len(messages) % 100 == 0:
                    logger.info(f"Consumed {len(messages)} messages so far")
                    
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                continue
        
        consumer.close()
        logger.info(f" >> Total messages consumed: {len(messages)}")
        
        # Log sample for debugging
        if messages:
            sample = messages[0]
            logger.info(f"Sample: {sample.get('sensor_id')} = {sample.get('value')} {sample.get('unit')}")
        
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)
        messages = []
    
    context['ti'].xcom_push(key='messages', value=messages)
    return len(messages)

def write_to_mongodb(**context):
    """Write messages to MongoDB Atlas"""
    logger.info("=" * 50)
    logger.info("WRITING TO MONGODB ATLAS")
    logger.info("=" * 50)
    
    if not CLOUD_AVAILABLE:
        logger.warning("Cloud libraries not available")
        return
    
    messages = context['ti'].xcom_pull(key='messages', task_ids='consume_from_kafka')
    
    if not messages:
        logger.warning("No messages to write to MongoDB")
        return
    
    MONGODB_ATLAS_URL = os.environ.get('MONGODB_ATLAS_URL')
    if not MONGODB_ATLAS_URL:
        logger.error("MONGODB_ATLAS_URL not set")
        return
    
    try:
        logger.info(f"Connecting to MongoDB Atlas...")
        client = MongoClient(MONGODB_ATLAS_URL)
        db = client['edge_platform_bronze']
        collection = db['raw_sensor_telemetry']
        
        docs = []
        for msg in messages:
            doc = msg.copy()
            doc['_bronze_ingested_at'] = datetime.now().isoformat()
            doc['_bronze_source'] = 'airflow_dag_confluent'
            docs.append(doc)
        
        if docs:
            result = collection.insert_many(docs)
            logger.info(f">> MongoDB: {len(result.inserted_ids)} documents inserted")
        else:
            logger.warning("No documents to insert")
        
        client.close()
        
    except Exception as e:
        logger.error(f"MongoDB error: {e}", exc_info=True)

def write_to_supabase(**context):
    """Write validated messages to Supabase"""
    logger.info("=" * 50)
    logger.info("WRITING TO SUPABASE")
    logger.info("=" * 50)
    
    if not CLOUD_AVAILABLE:
        logger.warning("Cloud libraries not available")
        return
    
    messages = context['ti'].xcom_pull(key='messages', task_ids='consume_from_kafka')
    
    if not messages:
        logger.warning("No messages to write to Supabase")
        return
    
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    if not SUPABASE_URL:
        logger.error("SUPABASE_URL not set")
        return
    
    try:
        logger.info(f"Connecting to Supabase...")
        conn = psycopg2.connect(SUPABASE_URL)
        cur = conn.cursor()
        
        # Ensure table exists
        cur.execute("""
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
            )
        """)
        
        count = 0
        for msg in messages:
            if not msg.get('validated', False):
                continue
            
            sensor_id = msg.get('sensor_id', 'unknown')
            sensor_type = sensor_id.split('_')[0] if '_' in sensor_id else 'unknown'
            
            cur.execute("""
                INSERT INTO dataops.silver_sensor_data 
                (sensor_id, sensor_type, value, unit, checksum, validated, source_timestamp, batch_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sensor_id,
                sensor_type,
                msg.get('value'),
                msg.get('unit'),
                msg.get('checksum'),
                msg.get('validated', False),
                datetime.fromtimestamp(msg.get('timestamp', datetime.now().timestamp())),
                f"airflow_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            ))
            count += 1
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f">> Supabase: {count} validated records inserted")
        
    except Exception as e:
        logger.error(f"Supabase error: {e}", exc_info=True)

def create_local_summary(**context):
    """Create local summary file"""
    messages = context['ti'].xcom_pull(key='messages', task_ids='consume_from_kafka')
    
    if not messages:
        logger.warning("No messages to summarize")
        return
    
    total = len(messages)
    validated = sum(1 for m in messages if m.get('validated', False))
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_messages": total,
        "validated_count": validated,
        "failed_count": total - validated,
        "validation_rate": (validated / total * 100) if total > 0 else 0,
    }
    
    output_dir = "/tmp/airflow_output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("=" * 50)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Total Messages: {total}")
    logger.info(f"Validated: {validated}")
    logger.info(f"Validation Rate: {summary['validation_rate']:.1f}%")
    logger.info(f"Summary saved to: {output_file}")
    logger.info("=" * 50)

dag = DAG(
    'sensor_data_pipeline_production',
    default_args=default_args,
    description='Production: Kafka → MongoDB + Supabase (Confluent Kafka)',
    schedule_interval='*/2 * * * *',  # Every 2 minutes
    catchup=False,
    max_active_runs=1,
    tags=['production', 'sensors', 'mongodb', 'supabase', 'confluent'],
)

consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_from_kafka,
    dag=dag,
)

mongodb_task = PythonOperator(
    task_id='write_to_mongodb',
    python_callable=write_to_mongodb,
    dag=dag,
)

supabase_task = PythonOperator(
    task_id='write_to_supabase',
    python_callable=write_to_supabase,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='create_local_summary',
    python_callable=create_local_summary,
    dag=dag,
)

consume_task >> [mongodb_task, supabase_task] >> summary_task

