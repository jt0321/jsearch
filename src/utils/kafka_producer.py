from kafka import KafkaProducer
import json
from ..config import KAFKA_BROKER, KAFKA_TOPIC

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_to_kafka(message):
    producer.send(KAFKA_TOPIC, value=message)
    producer.flush()
