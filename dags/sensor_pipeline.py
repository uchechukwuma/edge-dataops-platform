"""
Sensor Data Pipeline DAG
Reads from Kafka, transforms sensor data, prepares for cloud storage
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import logging
import os

# Default arguments
default_args = {
    'owner': 'dataops',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def consume_from_kafka(**context):
    """Consume messages from Kafka topic"""
    from kafka import KafkaConsumer
    import json
    
    logger = logging.getLogger(__name__)
    
    # IMPORTANT: Use the Docker service name, not localhost!
    KAFKA_BROKER = "kafka_broker:9092"  # <-- Changed from localhost to kafka_broker
    KAFKA_TOPIC = "sensor-data"
    
    stats = {
        "total_processed": 0,
        "validated": 0,
        "failed_validation": 0,
        "sensor_types": {}
    }
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info(f"✅ Connected to Kafka at {KAFKA_BROKER}")
        logger.info(f"Consuming from topic: {KAFKA_TOPIC}")
        
        for message in consumer:
            data = message.value
            sensor_id = data.get('sensor_id', 'unknown')
            sensor_type = sensor_id.split('_')[0] if '_' in sensor_id else 'unknown'
            validated = data.get('validated', False)
            
            stats["total_processed"] += 1
            if validated:
                stats["validated"] += 1
            else:
                stats["failed_validation"] += 1
            stats["sensor_types"][sensor_type] = stats["sensor_types"].get(sensor_type, 0) + 1
            
            if stats["total_processed"] % 1000 == 0:
                logger.info(f"Processed {stats['total_processed']} messages...")
        
        logger.info(f"📊 Final Stats - Total: {stats['total_processed']} | Validated: {stats['validated']} | Failed: {stats['failed_validation']}")
        logger.info(f"📊 Sensor types: {stats['sensor_types']}")
        
        context['ti'].xcom_push(key='stats', value=stats)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise

def aggregate_data(**context):
    """Aggregate sensor data"""
    import json
    import os
    from datetime import datetime
    
    logger = logging.getLogger(__name__)
    stats = context['ti'].xcom_pull(key='stats', task_ids='consume_from_kafka')
    
    if not stats:
        logger.warning("No statistics found")
        return
    
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_messages": stats["total_processed"],
        "validation_success_rate": (stats["validated"] / stats["total_processed"] * 100) if stats["total_processed"] > 0 else 0,
        "sensor_breakdown": stats["sensor_types"]
    }
    
    output_dir = "/tmp/airflow_output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = f"{output_dir}/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(output_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info(f"✅ Summary saved to: {output_file}")
    logger.info(f"📊 Summary: {json.dumps(summary, indent=2)}")
    
    context['ti'].xcom_push(key='summary_file', value=output_file)

def prepare_for_cloud(**context):
    """Prepare for cloud storage (Week 5)"""
    logger = logging.getLogger(__name__)
    logger.info("☁️ Data ready for cloud storage (Supabase + MongoDB Atlas)")
    logger.info("Week 5 will implement actual cloud upload")
    return "Ready for Week 5"

# Define DAG
dag = DAG(
    'sensor_data_pipeline',
    default_args=default_args,
    description='Process sensor data from Kafka',
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['sensors', 'kafka', 'industry40'],
)

consume_task = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_from_kafka,
    dag=dag,
)

aggregate_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    dag=dag,
)

cloud_task = PythonOperator(
    task_id='prepare_for_cloud',
    python_callable=prepare_for_cloud,
    dag=dag,
)

consume_task >> aggregate_task >> cloud_task
