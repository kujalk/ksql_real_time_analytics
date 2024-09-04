
# Create Kafka cluster and KSQL engine
docker compose up -d

# Run ksqldb-cli
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088

# To list the topics 
kafka-topics --bootstrap-server localhost:29092 --list

# To read the content from the topic from start 
kafka-console-consumer --bootstrap-server localhost:29092 --topic transactions --from-beginning

# To send the data to topic 
kafka-console-producer --broker-list localhost:29092 --topic transactions
> {"TXN_ID": 2011, "USERNAME": "arawind", "RECIPIENT": "Verizon", "AMOUNT": 220,"TS":"2024-09-03 13:54:36"}

# create kafka topic with custom partitions
kafka-topics --bootstrap-server localhost:29092 --topic <YOUR_TOPIC> --create --partitions <NUMBER>

# delete kafka topic
kafka-topics --bootstrap-server localhost:29092 --delete --topic <YOUR_TOPIC>

# To send random message from Python client
pip install kafka-python==2.0.2
python producer.py -no_of_messages 100 -no_of_anomalies 10

