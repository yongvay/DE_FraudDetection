# Author: Ng Yong Vay
import os
import csv
import json
import time
import logging
import threading
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RealTimeIngestor:
    def __init__(self, bootstrap_servers: str, topic: str, num_partitions: int = 3, replication_factor: int = 1):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers 
        
        self._create_topic_if_missing(
            topic_name=self.topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            client_id=f'wsl-{self.topic}-producer',
            acks='all',  
            retries=3,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None
        )
        logger.info(f"Initialized Kafka Producer connected to {bootstrap_servers} for topic '{self.topic}'")

    def _create_topic_if_missing(self, topic_name: str, num_partitions: int, replication_factor: int):
        admin_client = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers, 
                client_id=f'wsl-{topic_name}-admin'
            )
            topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic '{topic_name}' created successfully.")
            
        except TopicAlreadyExistsError:
            logger.info(f"Topic '{topic_name}' already exists. Skipping creation.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic_name}': {e}")
        finally:
            if admin_client:
                admin_client.close()

    def _cast_data_types(self, row: dict) -> dict:
        try:
            row['amount'] = float(row['amount']) if row.get('amount') else 0.0
            row['is_fraud'] = str(row.get('is_fraud', '')).strip().lower() == 'true'
            row['velocity_score'] = int(row['velocity_score']) if row.get('velocity_score') else 0
            row['spending_deviation_score'] = float(row['spending_deviation_score']) if row.get('spending_deviation_score') else 0.0
            row['geo_anomaly_score'] = float(row['geo_anomaly_score']) if row.get('geo_anomaly_score') else 0.0
            
            for key in ['fraud_type', 'time_since_last_transaction']:
                if not row.get(key) or str(row.get(key)).strip() == '':
                    row[key] = None
                elif key == 'time_since_last_transaction':
                    row[key] = float(row[key])

        except ValueError as e:
            logger.warning(f"Data casting error for transaction {row.get('transaction_id')}: {e}")
            
        return row

    def publish_transaction(self, raw_row: dict):
        try:
            clean_data = self._cast_data_types(raw_row)
            key = clean_data.get('transaction_id', 'unknown')
            
            future = self.producer.send(
                topic=self.topic,
                key=key,
                value=clean_data
            )

            future.get(timeout=10)
            
        except Exception as e:
            logger.error(f"Error publishing transaction: {e}")

    def stream(self, file_path: str, delay_seconds: float = 0.5):
        logger.info(f"Starting to stream data from {file_path}")
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    self.publish_transaction(row)
                    time.sleep(delay_seconds)
                    
            self.producer.flush()
            logger.info(f"Finished streaming data to {self.topic}.")
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Verify your path.")

if __name__ == "__main__":
    KAFKA_BROKER = 'localhost:9092'

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    basic_csv_path = os.path.join(base_dir, 'data', '5000_New_Basic.csv')
    advanced_csv_path = os.path.join(base_dir, 'data', 'sample_1500_sorted.csv')

    streamer_a = RealTimeIngestor(
        bootstrap_servers=KAFKA_BROKER, 
        topic='financial-transactions', 
        num_partitions=3, 
        replication_factor=1
    )

    streamer_b = RealTimeIngestor(
        bootstrap_servers=KAFKA_BROKER, 
        topic='ip-account-detection',
        num_partitions=3, 
        replication_factor=1
    )

    thread_a = threading.Thread(
        target=streamer_a.stream, 
        args=(basic_csv_path, 0.2)
    )
    
    thread_b = threading.Thread(
        target=streamer_b.stream, 
        args=(advanced_csv_path, 0.2)
    )
   
    print("Starting simultaneous streams...")
    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()
    
    print("All streams finished.")