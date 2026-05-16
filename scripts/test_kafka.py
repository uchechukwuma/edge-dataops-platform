#!/usr/bin/env python3
from kafka import KafkaConsumer
import json

try:
    consumer = KafkaConsumer(
        'sensor-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        consumer_timeout_ms=3000,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("✅ Connected to Kafka")
    
    count = 0
    for msg in consumer:
        print(f"Message: {msg.value.get('sensor_id')} - checksum: {msg.value.get('checksum')}")
        count += 1
        if count >= 5:
            break
    
    if count == 0:
        print("⚠️ No messages found. Run simulator first.")
    else:
        print(f"✅ Read {count} messages")
except Exception as e:
    print(f"❌ Error: {e}")
