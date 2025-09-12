# kafka_consumer.py
import json
import pandas as pd
from kafka import KafkaConsumer
import os

# ----------------------------
# Configuration
# ----------------------------
TOPIC = "steam-games"
BOOTSTRAP_SERVERS = "localhost:9092"
BATCH_SIZE = 1000
OUTPUT_FILE = "../data/raw_games_batch.csv"

# Ensure output directory exists
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

# ----------------------------
# Safe JSON loader
# ----------------------------
def safe_json_loads(x):
    try:
        return json.loads(x.decode('utf-8'))
    except (json.JSONDecodeError, AttributeError):
        return None  # skip invalid messages

# ----------------------------
# Kafka consumer
# ----------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=safe_json_loads,
    consumer_timeout_ms=10000  # stop after 10s of inactivity
)

# ----------------------------
# Consume messages
# ----------------------------
games_data = []
total_consumed = 0

print("Consuming messages from Kafka...")

for message in consumer:
    if message.value is None:
        continue
    games_data.append(message.value)
    total_consumed += 1

# Convert and save in batches
if len(games_data) >= BATCH_SIZE:
    df_batch = pd.DataFrame(games_data)
    df_batch.to_csv(
        OUTPUT_FILE,
        mode='a',
        index=False,
        header=not os.path.exists(OUTPUT_FILE),
        encoding='utf-8'
    )
    games_data = []
    print(f"{total_consumed} messages consumed and saved to CSV")

# Save remaining messages
if games_data:
    df_batch = pd.DataFrame(games_data)
    df_batch.to_csv(
        OUTPUT_FILE,
        mode='a',
        index=False,
        header=not os.path.exists(OUTPUT_FILE),
        encoding='utf-8'
    )


consumer.close()
print(f"Kafka consumption finished. Total messages consumed: {total_consumed}")
print(f"Saved to: {OUTPUT_FILE}")

