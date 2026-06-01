#!/usr/bin/env python3
"""
Sensor Simulator with C-Extension Validation and Retry Logic
"""

import json
import time
import random
import sys
import os

sys.path.append('/app')

import paho.mqtt.client as mqtt

try:
    import validator
    print(">> C-Validator loaded successfully")
except ImportError as e:
    print(f">> C-Validator not loaded: {e}")
    class validator:
        @staticmethod
        def validate(data_str):
            checksum = 0
            for ch in data_str.encode('utf-8'):
                checksum ^= ch
                checksum = ((checksum << 1) | (checksum >> 7)) & 0xFF
            return f"{checksum:02X}" # Clean modern format string assignment

MQTT_BROKER = os.environ.get('MQTT_BROKER', 'emqx_broker')
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/telemetry"

SENSOR_IDS = ["temp_sensor_01", "pressure_sensor_02", "vibration_sensor_03", 
              "flow_sensor_04", "humidity_sensor_05"]

RATE_PER_SECOND = 100

def generate_sensor_data():
    sensor_id = random.choice(SENSOR_IDS)
    if "temp" in sensor_id:
        value = round(random.uniform(18.0, 35.0), 2)
        unit = "C"
    elif "pressure" in sensor_id:
        value = round(random.uniform(980, 1050), 2)
        unit = "hPa"
    elif "vibration" in sensor_id:
        value = round(random.uniform(0, 10), 2)
        unit = "mm/s"
    elif "flow" in sensor_id:
        value = round(random.uniform(0, 100), 2)
        unit = "L/min"
    else:
        value = round(random.uniform(30, 80), 2)
        unit = "%"
    return {
        "sensor_id": sensor_id,
        "value": value,
        "unit": unit,
        "timestamp": time.time(),
        "source": "simulator"
    }

def connect_with_retry(client, max_retries=10, delay=5):
    for attempt in range(max_retries):
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            print(f">> Connected to MQTT broker after {attempt + 1} attempts")
            return True
        except Exception as e:
            print(f"Connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay)
    return False

def main():
    client = mqtt.Client()
    if not connect_with_retry(client):
        print(">> Failed to connect to MQTT broker after multiple attempts")
        sys.exit(1)
    client.loop_start()
    
    print(f">> Sensor Simulator Started")
    print(f"   MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"   Topic: {MQTT_TOPIC}")
    print(f"   Rate: {RATE_PER_SECOND} msg/sec\n")
    
    message_count = 0
    start_time = time.time()
    
    try:
        while True:
            raw_data = generate_sensor_data()
            data_str = f"{raw_data['sensor_id']}|{raw_data['value']}|{raw_data['unit']}"
            
            # 1. Get the integer checksum primitive back from C
            checksum_int = validator.validate(data_str)
            
            # 2. Format it to hex string safely inside Python's robust environment
            real_hex_checksum = f"{int(checksum_int):02X}"
            
            raw_data["checksum"] = real_hex_checksum
            raw_data["validated"] = True
            
            payload = json.dumps(raw_data)
            client.publish(MQTT_TOPIC, payload)
            message_count += 1
            time.sleep(1.0 / RATE_PER_SECOND)
            
            if message_count % 1000 == 0:
                elapsed = time.time() - start_time
                actual_rate = message_count / elapsed
                print(f">> Sent {message_count} msgs | Rate: {actual_rate:.0f} msg/sec | Last checksum: {real_hex_checksum}")
    except KeyboardInterrupt:
        print(f"\n\n>> Simulator stopped. Total messages sent: {message_count}")
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    main()