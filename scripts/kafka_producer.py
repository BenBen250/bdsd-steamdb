import json
from kafka import KafkaProducer

# JSONL file generated earlier
input_file = "../data/games_clean.jsonl"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    buffer_memory=67108864,  # larger buffer
    batch_size=32768,        # batch messages
    linger_ms=10             # small wait to improve batching
)

topic = "steam-games"

count = 0
check_every = 10000  # check Kafka ack every 10k messages

# Read the JSONL file and send each line to Kafka
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        game = json.loads(line)

        future = producer.send(topic, value=game)
        count += 1

        # Hybrid check: verify Kafka ack every N messages
        if count % check_every == 0:
            try:
                future.get(timeout=10)
                print(f"✅ Verified up to {count} messages sent so far...")
            except Exception as e:
                print(f"❌ Failed around message {count}: {e}")
                break

        if count % 1000 == 0:
            print(f"Sent {count} messages so far...")

# Flush remaining messages
producer.flush()
print(f"✅ All {count} messages sent to Kafka topic: {topic}")
