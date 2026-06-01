"""
CLEAN PRODUCTION DAG - Kafka to MongoDB and Supabase
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
import psycopg2
import json
import os
import logging

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

def consume_and_write(**context):
    """Consume from Kafka and write to both databases"""
    
    logger.info("=" * 50)
    logger.info("STARTING KAFKA CONSUMER")
    logger.info("=" * 50)
    
    # Kafka configuration
    conf = {
        'bootstrap.servers': 'kafka_broker:9092',
        'group.id': 'clean_production_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 10000,
    }
    
    messages = []
    max_messages = 500
    
    try:
        # Consume messages
        consumer = Consumer(conf)
        consumer.subscribe(['sensor-data'])
        
        logger.info(f"Connected to Kafka, polling for up to {max_messages} messages...")
        
        while len(messages) < max_messages:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                logger.info(f"No more messages, consumed {len(messages)} total")
                break
                
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Error: {msg.error()}")
                continue
            
            # Parse message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                messages.append(data)
                
                if len(messages) % 100 == 0:
                    logger.info(f"Consumed {len(messages)} messages so far")
                    
            except Exception as e:
                logger.warning(f"Parse error: {e}")
                continue
        
        consumer.close()
        logger.info(f"✅ Total messages consumed: {len(messages)}")
        
        if not messages:
            logger.warning("No messages consumed")
            return 0
            
        # Sample the first message
        sample = messages[0]
        logger.info(f"Sample: {sample.get('sensor_id')} = {sample.get('value')} {sample.get('unit')}")
        
    except Exception as e:
        logger.error(f"Kafka error: {e}", exc_info=True)
        return 0
    
    # Write to MongoDB
    mongodb_url = os.environ.get('MONGODB_ATLAS_URL')
    if mongodb_url:
        try:
            client = MongoClient(mongodb_url)
            db = client['edge_platform_bronze']
            collection = db['raw_sensor_telemetry']
            
            for msg in messages:
                msg['_ingested_at'] = datetime.now().isoformat()
                msg['_source'] = 'clean_production_dag'
            
            result = collection.insert_many(messages)
            logger.info(f"✅ MongoDB: Inserted {len(result.inserted_ids)} documents")
            client.close()
        except Exception as e:
            logger.error(f"MongoDB error: {e}")
    
    # Write to Supabase
    supabase_url = os.environ.get('SUPABASE_URL')
    if supabase_url:
        try:
            conn = psycopg2.connect(supabase_url)
            cur = conn.cursor()
            
            # Create table if needed
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS dataops;
                CREATE TABLE IF NOT EXISTS dataops.silver_sensor_data (
                    id SERIAL PRIMARY KEY,
                    sensor_id VARCHAR(100),
                    sensor_type VARCHAR(50),
                    value DOUBLE PRECISION,
                    unit VARCHAR(20),
                    checksum VARCHAR(100),
                    validated BOOLEAN,
                    source_timestamp TIMESTAMP,
                    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            count = 0
            for msg in messages:
                if msg.get('validated', False):
                    sensor_id = msg.get('sensor_id', 'unknown')
                    sensor_type = sensor_id.split('_')[0] if '_' in sensor_id else 'unknown'
                    
                    cur.execute("""
                        INSERT INTO dataops.silver_sensor_data 
                        (sensor_id, sensor_type, value, unit, checksum, validated, source_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        sensor_id,
                        sensor_type,
                        msg.get('value'),
                        msg.get('unit'),
                        msg.get('checksum'),
                        msg.get('validated', False),
                        datetime.fromtimestamp(msg.get('timestamp', datetime.now().timestamp()))
                    ))
                    count += 1
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"✅ Supabase: Inserted {count} validated records")
            
        except Exception as e:
            logger.error(f"Supabase error: {e}")
    
    # Summary
    logger.info("=" * 50)
    logger.info(f"PIPELINE SUMMARY: {len(messages)} messages processed")
    logger.info("=" * 50)
    
    return len(messages)

dag = DAG(
    'clean_production_dag',
    default_args=default_args,
    description='Clean: Kafka to MongoDB + Supabase',
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
)

consume_task = PythonOperator(
    task_id='consume_and_write',
    python_callable=consume_and_write,
    dag=dag,
)

