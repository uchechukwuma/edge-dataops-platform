#!/usr/bin/env python3
"""
Sensor Simulator with C-Extension Validation
Generates fake sensor data, validates via C-extension, publishes to EMQX
"""

import json
import time
import random
import sys
import os

# Add c_library to path for validator module
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'c_library'))

import paho.mqtt.client as mqtt
import validator

# Configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensors/telemetry"

SENSOR_IDS = ["temp_sensor_01", "pressure_sensor_02", "vibration_sensor_03", 
              "flow_sensor_04", "humidity_sensor_05"]

RATE_PER_SECOND = 100  # Start conservative, increase later

def generate_sensor_data():
    """Generate random realistic sensor reading"""
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
    else:  # humidity
        value = round(random.uniform(30, 80), 2)
        unit = "%"
    
    return {
        "sensor_id": sensor_id,
        "value": value,
        "unit": unit,
        "timestamp": time.time(),
        "source": "simulator"
    }

def main():
    # Connect to MQTT broker
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    
    print(f">> Sensor Simulator Started")
    print(f"   MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    print(f"   Topic: {MQTT_TOPIC}")
    print(f"   Rate: {RATE_PER_SECOND} msg/sec")
    print(f"   C-Validator: >> Loaded\n")
    
    message_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate raw data
            raw_data = generate_sensor_data()
            
            # Convert to string for validation
            data_str = f"{raw_data['sensor_id']}|{raw_data['value']}|{raw_data['unit']}"
            
            # Validate using C-extension
            checksum = validator.validate(data_str)
            
            # Add checksum to payload
            raw_data["checksum"] = checksum
            raw_data["validated"] = True
            
            # Publish to MQTT
            payload = json.dumps(raw_data)
            client.publish(MQTT_TOPIC, payload)
            
            message_count += 1
            
            # Rate limiting
            time.sleep(1.0 / RATE_PER_SECOND)
            
            # Progress report every 1000 messages
            if message_count % 1000 == 0:
                elapsed = time.time() - start_time
                actual_rate = message_count / elapsed
                print(f">> Sent {message_count} msgs | Rate: {actual_rate:.0f} msg/sec | Last checksum: {checksum}")
                
    except KeyboardInterrupt:
        print(f"\n\n>> Simulator stopped. Total messages sent: {message_count}")
    
    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    main()