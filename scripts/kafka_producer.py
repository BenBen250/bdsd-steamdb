import json
import time
from kafka import KafkaProducer
import pandas as pd

# Load your Steam games CSV
df = pd.read_csv("../data/games_clean.csv")  # fixed path for script location

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send each row as a message with a 1-second interval
for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('steam-games', value=data)
    print(f"Sent: {data['appid']} - {data['name']}")
    time.sleep(1)

producer.flush()
