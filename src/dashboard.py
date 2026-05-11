from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import threading
from confluent_kafka import Consumer
import json

app = FastAPI()

# Global state to store stats
stats = {"total_logs": 0, "anomalies": 0, "latest_status": "Starting..."}

def kafka_listener():
    """Background thread to update stats from Kafka."""
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dashboard-group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe(['system-logs'])
    
    while True:
        msg = c.poll(1.0)
        if msg is None: continue
        
        stats["total_logs"] += 1
        log = json.loads(msg.value().decode('utf-8'))
        
        # Simple threshold for dashboard (UI logic)
        if log['cpu_usage_pct'] > 80:
            stats["anomalies"] += 1
            stats["latest_status"] = "⚠️ ANOMALY DETECTED"
        else:
            stats["latest_status"] = "✅ System Healthy"

# Start Kafka listener in a background thread
threading.Thread(target=kafka_listener, daemon=True).start()

@app.get("/", response_class=HTMLResponse)
async def read_root():
    return f"""
    <html>
        <head><title>System Monitor</title><meta http-equiv="refresh" content="2"></head>
        <body style="font-family: Arial; text-align: center; padding: 50px;">
            <h1>Real-Time Anomaly Monitor</h1>
            <div style="font-size: 24px;">
                <p>Total Logs Processed: <b>{stats['total_logs']}</b></p>
                <p>Anomalies Found: <b style="color: red;">{stats['anomalies']}</b></p>
                <hr>
                <p>Current Status: <b>{stats['latest_status']}</b></p>
            </div>
        </body>
    </html>
    """