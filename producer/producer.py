from kafka import KafkaProducer
import json
import time
import pandas as pd
import os

KAFKA_TOPIC = "stock-market-data"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

total_sent = 0

for symbol in SYMBOLS:
    csv_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.exists(csv_path):
        print(f"WARNING: {csv_path} not found — skipping {symbol}")
        continue

    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        message = row.to_dict()
        producer.send(KAFKA_TOPIC, value=message)
        total_sent += 1
        time.sleep(0.1)

        if total_sent % 100 == 0:
            print(f"Sent {total_sent} records (current: {symbol})")

producer.flush()
print(f"All data sent successfully — {total_sent} total records.")
