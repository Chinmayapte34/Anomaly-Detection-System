import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Kafka Configuration
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    """ Callback for checking if the message was sent successfully """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # Standard SDE practice: print success in a readable format
        pass 

def generate_log(is_anomaly=False):
    """Generates system logs with 4 features for the ML model."""
    if is_anomaly:
        cpu_usage = random.uniform(85.0, 99.9)
        mem_usage = random.uniform(80.0, 98.0)
        latency = random.randint(1000, 5000)
        errors = random.randint(5, 20)
    else:
        cpu_usage = random.uniform(10.0, 45.0)
        mem_usage = random.uniform(20.0, 55.0)
        latency = random.randint(20, 250)
        errors = random.randint(0, 1)

    return {
        "timestamp": datetime.now().isoformat(),
        "device_id": "server-01",
        "cpu_usage_pct": round(cpu_usage, 2),
        "mem_usage_pct": round(mem_usage, 2),
        "latency_ms": latency,
        "error_count": errors
    }

print("🚀 Producer started. Broadcasting to Kafka...")
print("📝 Logging data to 'system_logs.json' for model training.")

try:
    while True:
        # 10% chance of an anomaly
        is_anomaly = random.random() < 0.10
        log = generate_log(is_anomaly)
        
        # 1. Send to Kafka
        producer.produce(
            'system-logs', 
            json.dumps(log).encode('utf-8'), 
            callback=delivery_report
        )
        producer.poll(0) # Serve delivery callbacks

        # 2. Save to local file for initial ML training
        with open("system_logs.json", "a") as f:
            f.write(json.dumps(log) + "\n")
            
        print(f"Sent: {'⚠️ ANOMALY' if is_anomaly else '✅ Normal'} - CPU: {log['cpu_usage_pct']}%")
        
        time.sleep(1) # Send 1 log per second

except KeyboardInterrupt:
    print("\nStopping Producer...")
finally:
    producer.flush() # Ensure all messages are delivered before exiting