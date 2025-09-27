from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SteamGamesPreprocess") \
    .getOrCreate()

# Read CSV file
df = spark.read.option("multiline", "true").csv("temp_games_1758891416.csv", header=True, inferSchema=True)

# Preprocessing: drop duplicates
df = df.dropDuplicates()

# Show schema and sample data
df.printSchema()
df.show(5)

# Save processed data locally as Parquet
df.write.mode("overwrite").parquet("../data/steam_games_processed")

print("Processed data saved locally at ../data/steam_games_processed")

# Stop Spark session
spark.stop()