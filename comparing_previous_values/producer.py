import json
import random
import time
from kafka import KafkaProducer
import argparse

def get_random_record():
    """Generate a random record with server, transactiontime, and testsource."""
    record = {
        "server": f"server{random.randint(1,2)}",
        "transactiontime": random.randint(20, 210),
        "testsource": f"probe-{random.randint(1,3)}"
    }
    return record

def create_anomaly_record(previous_record):
    """Create an anomaly record with transactiontime > 200 more than the previous one."""
    anomaly_record = previous_record.copy()
    anomaly_record["transactiontime"] = previous_record["transactiontime"] + random.randint(1000, 2800)
    return anomaly_record

def send_data_to_kafka(topic, no_of_records, no_of_anomalies):
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',  # Update to match your Kafka broker
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    # Generate random indices where anomalies will be inserted
    anomaly_indices = random.sample(range(no_of_records - 1), no_of_anomalies)

    i = 0
    while i < no_of_records:
        # Generate a random record
        record = get_random_record()

        # If this index should have an anomaly
        if i in anomaly_indices:
            print(f"Anomaly record at index {i} and {i+1}")
            # Send the normal record
            producer.send(topic, record)
            print(f"Sent normal record: {record}")
            # Generate and send the anomaly record
            anomaly_record = create_anomaly_record(record)
            producer.send(topic, anomaly_record)
            print(f"Sent anomaly record: {anomaly_record}")
            # Skip the next index (as the anomaly takes up two records)
            i += 2
        else:
            producer.send(topic, record)
            print(f"Sent normal record: {record}")
            i += 1

        # Optional: add a small delay to simulate processing time
        time.sleep(0.1)

    # Ensure all messages are sent
    producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer script")
    parser.add_argument('--no_of_records', type=int, required=True, help='Total number of records to send')
    parser.add_argument('--no_of_anomalies', type=int, required=True, help='Number of anomalies to send')

    args = parser.parse_args()

    # Ensure the number of anomalies is less than half of no_of_records to avoid overlap
    if args.no_of_anomalies * 2 > args.no_of_records:
        print("Number of anomalies is too large compared to the total records!")
        exit(1)

    # Send data to Kafka
    send_data_to_kafka('synthetic_transactions', args.no_of_records, args.no_of_anomalies)
