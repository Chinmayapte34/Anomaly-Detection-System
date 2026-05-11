🚀 Distributed Real-Time Anomaly Detection System
A high-performance monitoring pipeline that uses Machine Learning to detect system irregularities in real-time. Built with a microservices architecture to demonstrate scalability and SDE best practices.

🏗️ System Architecture
The system is divided into four main components:

Log Producer: Simulates a high-traffic server environment generating telemetry data (CPU, RAM, Latency).

Message Broker (Kafka): Orchestrates the data flow, ensuring high availability and fault tolerance.

ML Inference Engine: An Isolation Forest model that analyzes the stream to identify "outliers" without needing labeled data.

Live Dashboard: A FastAPI web interface providing real-time observability.

🛠️ Tech Stack
Language: Python 3.x

Infrastructure: Docker, Apache Kafka, Zookeeper

Machine Learning: Scikit-learn (Isolation Forest), Pandas

Backend: FastAPI, Uvicorn

DevOps: Docker Compose

🚀 Getting Started
1. Prerequisites
Docker Desktop installed and running.

Python 3.10+

2. Installation & Setup
Bash
# Clone the repository
git clone https://github.com/yourusername/Anomaly-Detection-System.git
cd Anomaly-Detection-System

# Start the Infrastructure (Kafka & Zookeeper)
docker-compose up -d

# Install dependencies
pip install -r requirements.txt
3. Running the Pipeline
Open three separate terminals:

Start Producer: python src/producer.py

Start ML Engine: python src/consumer.py

Start Dashboard: uvicorn src.dashboard:app --reload

Visit http://localhost:8000 to view the live monitor.

🧠 Key Engineering Challenges Solved
Decoupled Architecture: Used Kafka to separate data generation from processing, allowing the system to handle spikes in traffic.

Unsupervised Learning: Implemented Isolation Forest to detect "Zero-Day" anomalies where historical failure labels aren't available.

Containerization: Fully dockerized the environment to ensure "it works on my machine" works on yours too.