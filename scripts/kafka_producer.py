import json
from kafka import KafkaProducer

# JSONL file generated earlier
input_file = "../data/games_clean.jsonl"

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "steam-games"

# Read the JSONL file and send each line to Kafka
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        game = json.loads(line)
        producer.send(topic, value=game)

# Flush messages to make sure all are sent
producer.flush()
print("All messages sent to Kafka topic:", topic)

