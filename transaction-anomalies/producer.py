import random
import json
import datetime
import argparse
from kafka import KafkaProducer

class DataGenerator:
    def __init__(self,no_of_messages,no_of_anomalies,broker='localhost:29092',topic='transactions'):
        self.total_messages = no_of_messages
        self.total_anomalies = no_of_anomalies
        self.broker = broker 
        self.topic = topic

    def _create_message(self,recipient) -> dict:
        return {
            "TXN_ID": random.uniform(100, 15000),
            "USERNAME": f"user{random.randint(1, 1000)}",
            "RECIPIENT": recipient,
            "AMOUNT": random.uniform(10, 500),
            "TS": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def pump_data(self) -> None:
        producer = KafkaProducer(bootstrap_servers=self.broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # Specific recipients for anomalies
        specific_recipients = ['Verizon', 'Best Buy', 'Spirit Halloween']
        
        # Additional random recipients
        other_recipients = [f"RandomCompany{random.randint(1, 100)}" for _ in range(10)]

        # Send messages
        for i in range(self.total_messages):
            if self.total_anomalies > 0:
                recipient = random.choice(specific_recipients)
                self.total_anomalies -= 1
            else:
                recipient = random.choice(other_recipients)
            
            message = self._create_message(recipient)
            producer.send(self.topic, value=message)
            print(f"Sent: {message}")

        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Send messages to a Kafka topic.')
    parser.add_argument('-no_of_messages', type=int, required=True, help='Number of messages to send')
    parser.add_argument('-no_of_anomalies', type=int, required=True, help='Number of anomalies (specific recipients)')

    args = parser.parse_args()
    DG = DataGenerator(args.no_of_messages, args.no_of_anomalies)
    DG.pump_data()
    
