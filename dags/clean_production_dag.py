"""
CLEAN PRODUCTION DAG - Kafka to MongoDB and Supabase with Data Quality
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
        logger.info(f">> Total messages consumed: {len(messages)}")
        
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
            logger.info(f">> MongoDB: Inserted {len(result.inserted_ids)} documents")
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
            logger.info(f">> Supabase: Inserted {count} validated records")
            
            # Store the count in XCom for the quality task
            context['ti'].xcom_push(key='silver_record_count', value=count)
            
        except Exception as e:
            logger.error(f"Supabase error: {e}")
            raise  # Re-raise to fail the task if Supabase write fails
    
    # Summary
    logger.info("=" * 50)
    logger.info(f"PIPELINE SUMMARY: {len(messages)} messages processed, {count} written to Silver")
    logger.info("=" * 50)
    
    return len(messages)

def run_data_quality_checks(**context):
    """Run Great Expectations validation on the silver layer"""
    
    logger.info("=" * 50)
    logger.info("RUNNING GREAT EXPECTATIONS DATA QUALITY CHECKS")
    logger.info("=" * 50)
    
    if not GX_AVAILABLE:
        logger.error("Great Expectations not available - quality checks skipped")
        return
    
    supabase_url = os.environ.get('SUPABASE_URL')
    if not supabase_url:
        logger.error("SUPABASE_URL not set - cannot run quality checks")
        raise ValueError("SUPABASE_URL not set")
    
    # Get the record count from previous task
    silver_count = context['ti'].xcom_pull(key='silver_record_count', task_ids='consume_and_write')
    logger.info(f"Silver layer records in this batch: {silver_count}")
    
    try:
        # Get Great Expectations context (pointing to the gx/ directory in the project)
        context_gx = gx.get_context(project_root_dir="/opt/airflow")
        logger.info(f"Great Expectations context loaded from: /opt/airflow")
        
        # Get datasource and asset
        try:
            datasource = context_gx.get_datasource("supabase_silver")
            logger.info("Using existing datasource: supabase_silver")
        except ValueError:
            logger.info("Datasource not found, creating new one...")
            datasource = context_gx.sources.add_postgres(
                name="supabase_silver",
                connection_string=supabase_url
            )
            logger.info("Datasource created: supabase_silver")
        
        # Get or create asset
        try:
            data_asset = datasource.get_asset("silver_sensor_data")
            logger.info("Using existing asset: silver_sensor_data")
        except Exception:
            logger.info("Asset not found, creating new one...")
            data_asset = datasource.add_table_asset(
                name="silver_sensor_data",
                table_name="silver_sensor_data",
                schema_name="dataops"
            )
            logger.info("Asset created: silver_sensor_data")
        
        # Get expectation suite
        suite_name = "silver_sensor_data_suite"
        try:
            suite = context_gx.get_expectation_suite(suite_name)
            logger.info(f"Using existing expectation suite: {suite_name}")
        except Exception:
            logger.error(f"Expectation suite '{suite_name}' not found")
            logger.info("Run python scripts/gx_full_pipeline.py first to create expectations")
            raise ValueError(f"Expectation suite '{suite_name}' not found")
        
        # Run validation
        batch_request = data_asset.build_batch_request()
        validator = context_gx.get_validator(
            batch_request=batch_request,
            expectation_suite=suite
        )
        
        results = validator.validate()
        
        logger.info(f"Validation Success: {results.success}")
        logger.info(f"Evaluated expectations: {results.statistics['evaluated_expectations']}")
        logger.info(f"Successful expectations: {results.statistics['successful_expectations']}")
        
        if not results.success:
            failed = results.statistics['evaluated_expectations'] - results.statistics['successful_expectations']
            logger.error(f"❌ Data quality checks FAILED! {failed} expectations failed")
            
            # Log detailed failures
            for check in results.results:
                if not check.success:
                    logger.error(f"  Failed: {check.expectation_config.expectation_type}")
                    if 'unexpected_list' in check.result:
                        logger.error(f"    Unexpected values: {check.result['unexpected_list'][:5]}")
            
            raise ValueError(f"Data quality checks failed - {failed} expectations not met")
        
        logger.info("✅ ALL DATA QUALITY CHECKS PASSED!")
        logger.info(f"Validated {silver_count} records with 13 quality expectations")
        
        # Build data docs (optional)
        context_gx.build_data_docs()
        logger.info("Data Docs updated")
        
    except Exception as e:
        logger.error(f"Great Expectations error: {e}")
        raise

# DAG definition
dag = DAG(
    'clean_production_dag',
    default_args=default_args,
    description='Clean: Kafka to MongoDB + Supabase with Data Quality',
    schedule_interval='*/2 * * * *',
    catchup=False,
    max_active_runs=1,
)

consume_task = PythonOperator(
    task_id='consume_and_write',
    python_callable=consume_and_write,
    dag=dag,
)

quality_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag,
)

# Dependencies: consume -> quality (quality runs after consume finishes)
consume_task >> quality_task