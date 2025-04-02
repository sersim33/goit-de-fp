from kafka import KafkaConsumer
from kafka_config import kafka_config
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_3'
)

my_name = "Sergii_Simak"
topic_name = f'{my_name}_athlete_event_results'

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:
        print(f"Received message: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()