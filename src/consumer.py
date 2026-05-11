from confluent_kafka import Consumer
import json
from model import AnomalyDetector

# 1. Initialize and Train the Detector
detector = AnomalyDetector()
# We assume you've run the producer long enough to create this file
detector.train("system_logs.json") 

# 2. Kafka Setup
conf = {'bootstrap.servers': "localhost:9092", 'group.id': "detector-group", 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)
consumer.subscribe(['system-logs'])

print("AI Detector is Live... Monitoring Kafka Stream.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        log_data = json.loads(msg.value().decode('utf-8'))
        
        # 3. Predict Anomaly
        is_anomaly = detector.predict(log_data)
        
        if is_anomaly:
            print(f"⚠️  ANOMALY DETECTED: High Load on {log_data['device_id']} | CPU: {log_data['cpu_usage_pct']}%")
        else:
            print(f"✅ Normal: CPU {log_data['cpu_usage_pct']}%")

except KeyboardInterrupt:
    print("Shutting down...")
finally:
    consumer.close()