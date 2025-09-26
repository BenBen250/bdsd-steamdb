
import json
import pandas as pd
from kafka import KafkaProducer

# JSONL file generated earlier
input_file = "../data/games_clean.jsonl"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    buffer_memory=67108864,  
    batch_size=32768,        
    linger_ms=10             # small wait to improve batching
)

topic = "steam-games"

count = 0
check_every = 10000  


# Load JSONL into pandas DataFrame
df = pd.read_json(input_file, lines=True, encoding="utf-8")

# Example preprocessing: drop rows with missing 'name', fillna for 'price', drop 'unnecessary_column' if exists
if 'name' in df.columns:
    df = df.dropna(subset=['name'])
if 'price' in df.columns:
    df['price'] = df['price'].fillna(0)
if 'unnecessary_column' in df.columns:
    df = df.drop(columns=['unnecessary_column'])

# Send each row as a message to Kafka
for _, row in df.iterrows():
    game = row.to_dict()
    future = producer.send(topic, value=game)
    count += 1

    if count % check_every == 0:
        try:
            future.get(timeout=10)
            print(f"✅ Verified up to {count} messages sent so far...")
        except Exception as e:
            print(f"❌ Failed around message {count}: {e}")
            break

    if count % 1000 == 0:
        print(f"Sent {count} messages so far...")

producer.flush()
print(f"✅ All {count} messages sent to Kafka topic: {topic}")
