"""
Class for topics enumeration 
Topics:
    - sensor__sinusoidal_temperature_reading
    - database_sensor_data
"""

from enum import Enum
import json
from kafka import KafkaProducer
import os
import psycopg2

# Load DB credentials from env
pg_host = os.environ.get("PG_HOST", "localhost")
pg_port = os.environ.get("PG_PORT", "5432")
pg_database = os.environ.get("PG_DATABASE", "comp430project")
pg_user = os.environ.get("PG_USER", "")
pg_password = os.environ.get("PG_PASSWORD", "")

class TopicsEnum(Enum):
    ETH_POOL_DATA = "eth_pool_data"
    ETH_LOADER = "eth_loader"

broker = "localhost:9092"


def get_producer():
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    return producer

def get_broker():
    return broker

def get_connection():
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    return conn
