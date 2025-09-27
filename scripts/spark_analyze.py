from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max, min

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SteamGamesAnalyze") \
    .getOrCreate()

# Read processed data locally
df = spark.read.parquet("../data/steam_games_processed")

# Basic analysis
print("Total games:", df.count())
print("Average price:", df.select(avg("price")).collect()[0][0])
print("Max price:", df.select(max("price")).collect()[0][0])
print("Min price:", df.select(min("price")).collect()[0][0])

# Show top 10 games by price
df.orderBy("price", ascending=False).show(10)

# Stop Spark session
spark.stop()