"""
Class for topics enumeration 
Topics:
    - sensor__sinusoidal_temperature_reading
    - database_sensor_data
"""

from enum import Enum
import json
from kafka import KafkaProducer

class TopicsEnum(Enum):
    ETH_POOL_DATA = "eth_pool_data"

broker = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[broker],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_producer():
    return producer

def get_broker():
    return broker
