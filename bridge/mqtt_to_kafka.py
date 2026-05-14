#!/usr/bin/env python3
"""
MQTT to Kafka Bridge
Consumes messages from EMQX, produces to Kafka
"""

import json
import signal
import sys
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/telemetry"

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "sensor-data"

# Statistics
stats = {
    "received": 0,
    "produced": 0,
    "failed": 0
}

# Global producer variable
producer = None

def create_kafka_producer():
    """Create and return Kafka producer"""
    try:
        prod = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all'
        )
        print(f"✅ Connected to Kafka: {KAFKA_BROKER}")
        return prod
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return None

def on_connect(client, userdata, flags, rc):
    """MQTT connect callback"""
    print(f"✅ Connected to MQTT: {MQTT_BROKER}:{MQTT_PORT} (rc={rc})")
    client.subscribe(MQTT_TOPIC)
    print(f"📡 Subscribed to topic: {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    """MQTT message callback - forward to Kafka"""
    global stats, producer
    
    stats["received"] += 1
    
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        
        # Add bridge metadata
        payload["bridge_received_at"] = time.time()
        
        # Send to Kafka (use global producer directly)
        producer.send(KAFKA_TOPIC, payload)
        stats["produced"] += 1
        
        # Progress report every 1000 messages
        if stats["received"] % 1000 == 0:
            print(f"📊 Bridge Status | Received: {stats['received']} | Produced: {stats['produced']} | Failed: {stats['failed']}")
            
    except Exception as e:
        stats["failed"] += 1
        print(f"⚠️ Error processing message: {e}")

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print(f"\n\n📊 Final Bridge Statistics:")
    print(f"   Messages received from MQTT: {stats['received']}")
    print(f"   Messages produced to Kafka: {stats['produced']}")
    print(f"   Failed: {stats['failed']}")
    if stats['received'] > 0:
        success_rate = (stats['produced'] / stats['received']) * 100
        print(f"   Success rate: {success_rate:.2f}%")
    print("\n✅ Bridge stopped.")
    sys.exit(0)

def main():
    global producer
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create Kafka producer
    producer = create_kafka_producer()
    if producer is None:
        sys.exit(1)
    
    # Create MQTT client
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    print(f"\n MQTT → Kafka Bridge Started")
    print(f"   MQTT: {MQTT_BROKER}:{MQTT_PORT} → {MQTT_TOPIC}")
    print(f"   Kafka: {KAFKA_BROKER} → {KAFKA_TOPIC}\n")
    
    client.loop_forever()

if __name__ == "__main__":
    main()
