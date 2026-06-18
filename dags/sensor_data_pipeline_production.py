"""
Sensor Data Pipeline DAG - PRODUCTION GRADE with Confluent Kafka + Great Expectations
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

# Import Great Expectations
try:
    import great_expectations as gx
    GX_AVAILABLE = True
except ImportError:
    GX_AVAILABLE = False
    logging.warning("Great Expectations not available")

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
        
        # Store count for quality check
        context['ti'].xcom_push(key='silver_record_count', value=count)
        
    except Exception as e:
        logger.error(f"Supabase error: {e}", exc_info=True)
        raise

def run_data_quality_checks(**kwargs):
    """
    EN 50159 Safety Communication Layer: Semantic Diagnostics Unit.
    Validates boundary states before advancing data to the dbt Gold tier.
    """
    import logging
    import os
    import great_expectations as gx

    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("RUNNING GREAT EXPECTATIONS DATA QUALITY CHECKS (FIXED V1.X API)")
    logger.info("=" * 50)

    # 1. Load the persistent context inside the container path
    context = gx.get_context(context_root_dir="/opt/airflow/gx")
    
    # 2. Extract database connection parameters cleanly from environment
    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    if not SUPABASE_URL:
        # Secure fallback mapping to the cloud pooler port
        SUPABASE_URL = "postgresql://postgres.dkwhraqmdvdzfkyxoekk:Edge_dataop052026@aws-1-eu-central-2.pooler.supabase.com:6543/postgres"

    datasource_name = "supabase_silver_v3"
    asset_name = "silver_sensor_data"
    suite_name = "silver_sensor_data_suite"

    # 3. Idempotent Fluent Datasource Extraction / Creation
    try:
        datasource = context.data_sources.get(datasource_name)
        logger.info(f"Using existing fluent datasource: {datasource_name}")
    except Exception:
        logger.info(f"Creating missing fluent datasource: {datasource_name}")
        datasource = context.data_sources.add_postgres(
            name=datasource_name,
            connection_string=SUPABASE_URL
        )

    # 4. Idempotent Fluent Table Asset Extraction / Creation
    try:
        data_asset = datasource.get_asset(asset_name)
        logger.info(f"Using existing fluent data asset: {asset_name}")
    except Exception:
        logger.info(f"Creating missing fluent table asset: {asset_name}")
        data_asset = datasource.add_table_asset(
            name=asset_name,
            table_name="silver_sensor_data",
            schema_name="dataops"
        )

    # 5. Extract Expectation Suite
    try:
        suite = context.suites.get(suite_name)
        logger.info(f"Linked to safety expectation suite: {suite_name}")
    except Exception as e:
        logger.error(f"Critical: Expectation suite '{suite_name}' not initialized on disk.")
        raise ValueError(f"Missing suite metadata dependency: {str(e)}")

    # 6. Build batch request and initialize validator engine
    batch_request = data_asset.build_batch_request()
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    logger.info("Enforcing semantic boundary filters over Supabase records...")
    
    # 7. Execute the validation sweep
    results = validator.validate()
    
    evaluated = results.statistics['evaluated_expectations']
    successful = results.statistics['successful_expectations']
    
    logger.info(f"Validation Complete. Evaluated: {evaluated} | Successful: {successful}")

    # 8. Actuate Safety Safe-State Intercept Function
    if not results.success:
        failed_count = evaluated - successful
        logger.error("!" * 60)
        logger.error(f">> SEMANTIC BOUNDARY BREACH DETECTED: {failed_count} RULES FAILED!")
        logger.error("!" * 60)
        
        # Hard fail the task to prevent corrupt records from unlocking dbt runs
        raise ValueError(f"EN 50159 Safety Gate Intercept activated. Breached rules count: {failed_count}")

    logger.info(">> All semantic expectations passed validation. Compiling documentation matrices...")
    context.build_data_docs()
    return True

def create_local_summary(**context):
    """Create local summary file and log quality metrics"""
    # 1. Pull the messages from the Kafka task
    messages = context['ti'].xcom_pull(key='messages', task_ids='consume_from_kafka')
    
    # 2. Pull the pass/fail result from your Great Expectations task
    gx_passed = context['ti'].xcom_pull(task_ids='run_data_quality_checks')
    
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
        "gx_quality_gate_passed": gx_passed  # Added to your JSON artifact log!
    }
    
    # Path inside the container filesystem
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
    
    # FIX: Uses the pulled XCom variable instead of the undefined 'results' name
    logger.info(f"Quality Check: {'PASSED' if gx_passed else 'FAILED'}")
    logger.info(f"Summary saved to: {output_file}")
    logger.info("=" * 50)

dag = DAG(
    'sensor_data_pipeline_production',
    default_args=default_args,
    description='Production: Kafka → MongoDB + Supabase (Confluent Kafka) with Data Quality',
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['production', 'sensors', 'mongodb', 'supabase', 'confluent', 'data-quality'],
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

quality_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='create_local_summary',
    python_callable=create_local_summary,
    dag=dag,
)

# Task Dependencies
consume_task >> [mongodb_task, supabase_task] >> quality_task >> summary_task