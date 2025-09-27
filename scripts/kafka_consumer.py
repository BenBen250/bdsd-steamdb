import json
import csv
import os
import time
from kafka import KafkaConsumer
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

TOPIC = "steam-games"
BOOTSTRAP_SERVERS = "localhost:9092"
HDFS_PATH = "hdfs://localhost:9000/steam_games/"  # Adjust HDFS path as needed

# Safe JSON loader
def safe_json_loads(x):
    try:
        return json.loads(x.decode('utf-8'))
    except (json.JSONDecodeError, AttributeError):
        return None  # skip invalid messages

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=safe_json_loads
)

games_data = []
total_consumed = 0
start_time = time.time()

print("Consuming messages from Kafka and processing every minute...")

while True:
    messages = consumer.poll(timeout_ms=1000)  # Poll for messages, timeout 1 second
    for topic_partition, records in messages.items():
        for record in records:
            if record.value is None:
                continue
            games_data.append(record.value)
            total_consumed += 1

    current_time = time.time()
    if current_time - start_time >= 60 and games_data:
        # Process the batch
        print(f"Processing batch of {len(games_data)} messages at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}")

        # Write to temp CSV
        temp_csv = f"temp_games_{int(current_time)}.csv"
        df_batch = pd.DataFrame(games_data)
        df_batch.to_csv(temp_csv, index=False)

        # Spark processing
        spark = SparkSession.builder \
            .appName("SteamGamesProcessing") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()

        # Read CSV with Spark
        df = spark.read.csv(temp_csv, header=True, inferSchema=True)

        # Preprocessing: clean and remove unnecessary stuff
        df_processed = df \
            .filter(col("name").isNotNull()) \
            .withColumn("price", when(col("price").isNull(), 0).otherwise(col("price"))) \
            .dropna(subset=["appid"])  # Assuming appid is key field

        # Save processed data to HDFS (append mode for streaming effect)
        df_processed.write.mode("append").csv(HDFS_PATH + "processed_games")

        spark.stop()

        # Clean up temp file
        os.remove(temp_csv)

        print(f"Batch processed and saved to HDFS. Total consumed so far: {total_consumed}")

        # Reset for next interval
        games_data = []
        start_time = current_time

consumer.close()

