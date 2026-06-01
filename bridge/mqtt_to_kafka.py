#!/usr/bin/env python3
"""
MQTT to Kafka Bridge - Production Grade (Synchronous Delivery Fixed)
"""

import json
import signal
import sys
import time
import os

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_BROKER = os.environ.get('MQTT_BROKER', 'emqx_broker')
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/telemetry"

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka_broker:9092')
KAFKA_TOPIC = "sensor-data"

stats = {
    "received": 0,
    "produced": 0,
    "failed": 0
}

producer = None

def create_kafka_producer():
    try:
        prod = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            acks='all',
            # FORCE EXPLICIT VERSION PINNING
            api_version=(3, 7, 0)
        )
        print(f">> Connected Natively to Kafka: {KAFKA_BROKER}")
        return prod
    except Exception as e:
        print(f">> Failed to connect to Kafka: {e}")
        return None

def on_connect(client, userdata, flags, rc):
    print(f">> Connected to MQTT: {MQTT_BROKER}:{MQTT_PORT} (rc={rc})")
    client.subscribe(MQTT_TOPIC)
    print(f">> Subscribed to topic: {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    global stats, producer
    
    stats["received"] += 1
    
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        payload["bridge_received_at"] = time.time()
        
        if producer:
            max_send_retries = 3
            for attempt in range(max_send_retries):
                try:
                    # Send and block up to 5 seconds for cluster metadata ack
                    future = producer.send(KAFKA_TOPIC, payload)
                    meta = future.get(timeout=5) 
                    stats["produced"] += 1
                    break # Success! Break the retry loop
                except Exception as kafka_err:
                    if attempt == max_send_retries - 1:
                        # If all retries failed, log it and move on
                        raise kafka_err
                    print(f">> Kafka busy, retrying stream link ({attempt + 1}/{max_send_retries})...")
                    time.sleep(1)
        
        if stats["received"] % 100 == 0:
            print(f">> Real-time Bridge Status | Received: {stats['received']} | Confirmed In Kafka: {stats['produced']} | Failed: {stats['failed']}")
            
    except Exception as e:
        stats["failed"] += 1
        print(f">> Real-time Delivery Transmission Failure: {e}")
        
        # If the producer client is completely stale, attempt a clean reconstruction
        if "TimeoutError" in str(type(e)) or "timeout" in str(e).lower():
            print(">> Re-initializing stale Kafka Producer connection handle...")
            producer = create_kafka_producer()

def signal_handler(sig, frame):
    print(f"\n>> Final Stats - Received: {stats['received']} | Produced: {stats['produced']} | Failed: {stats['failed']}")
    sys.exit(0)

def main():
    global producer
    
    signal.signal(signal.SIGINT, signal_handler)
    
    producer = create_kafka_producer()
    if producer is None:
        print(">> Cannot start bridge without Kafka connection")
        sys.exit(1)
    
    #  THE FIXED LINE: Explicitly define CallbackAPIVersion to match the modern library spec
    from paho.mqtt import enums
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    
    print(f"\n>> MQTT → Kafka Native Bridge Active")
    client.loop_forever()

if __name__ == "__main__":
    main()