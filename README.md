# Steam Game Trend Analyzer

This application tracks and analyzes trending games on Steam.
It shows:
- Top trending games
- Genre popularity over time
- Player activity spikes

# how to start kafka at terminal
start zookeeper: 
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
start kafka server:
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
activate python virtual environment:
cd C:\Users\Benedict\Programming\steam-trend-analyzer
.\venv\Scripts\activate

# to create topics:
cd C:\kafka
.\bin\windows\kafka-topics.bat --create --topic steam-games --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
# verify topics: 
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
# Delete topic to reset the content in the port
.\bin\windows\kafka-topics.bat --delete --topic steam-games --bootstrap-server localhost:9092
# if encounter error delete the kafka-logs and zookeeper in c:\tmp folder


# Run the producer script : path to environment first
python kafka_producer.py

# Run the consumer script : path to environment first
python kafka_consumer.py

how to activate venv in terminal:
.\venv\Scripts\Activate.ps1


Step 4: Verify messages are sent
Open another PowerShell window.
Navigate to Kafka’s bin folder:
cd C:\kafka\bin\windows
Run the console consumer to see the messages in real time:
.\kafka-console-consumer.bat --topic steam-games --bootstrap-server localhost:9092 --from-beginning


