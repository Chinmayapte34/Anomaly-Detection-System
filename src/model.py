import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib # For saving the model

class AnomalyDetector:
    def __init__(self):
        # contamination=0.1 means we expect 10% of data to be anomalies
        self.model = IsolationForest(contamination=0.1, random_state=42)
        self.is_trained = False

    def train(self, data_file):
        """Trains the model using the logs collected in Phase 1."""
        try:
            df = pd.read_json(data_file, lines=True)
            # We only train on numerical features
            features = df[['cpu_usage_pct', 'mem_usage_pct', 'latency_ms', 'error_count']]
            
            self.model.fit(features)
            self.is_trained = True
            print("Model trained successfully on historical logs.")
        except Exception as e:
            print(f"Training failed: {e}")

    def predict(self, log_entry):
        """Predicts if a single log entry is an anomaly."""
        if not self.is_trained:
            return False # Default to normal if not trained
        
        # Prepare the single row for prediction
        df_single = pd.DataFrame([log_entry])
        features = df_single[['cpu_usage_pct', 'mem_usage_pct', 'latency_ms', 'error_count']]
        
        # IsolationForest returns -1 for anomalies and 1 for normal
        prediction = self.model.predict(features)
        return True if prediction[0] == -1 else False