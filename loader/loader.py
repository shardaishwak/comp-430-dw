from kafka import KafkaConsumer
import json
import sys 
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from kafka_utils import TopicsEnum, get_broker
import pandas as pd
import numpy as np
import os
import psycopg2
import datetime
from record import getTick

pg_host = os.environ.get("PG_HOST", "localhost")
pg_port = os.environ.get("PG_PORT", "5432")
pg_database = os.environ.get("PG_DATABASE", "comp430project")
pg_user = os.environ.get("PG_USER", "")
pg_password = os.environ.get("PG_PASSWORD", "")

conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=pg_database,
    user=pg_user,
    password=pg_password
)
cursor = conn.cursor()
print("Connected to PostgreSQL database!")

def digest(json_data):
    print(json_data)
    return
    insert_fact_query = """
        INSERT INTO fact_market_data
        (network_fees, close_price, open_price, high_price, low_price, trading_volume, 
         transaction_count, circulating_supply, market_cap, pair_key, exchange_key, asset_key, 
         date_key, ema, rsi, sma)
        VALUES (%(network_fees)s, %(close_price)s, %(open_price)s, %(high_price)s, %(low_price)s, 
                %(trading_volume)s, %(transaction_count)s, %(circulating_supply)s, %(market_cap)s, 
                %(pair_key)s, %(exchange_key)s, %(asset_key)s, %(date_key)s, %(ema)s, %(rsi)s, %(sma)s)
    """
    cursor.execute(insert_fact_query, json_data)
    conn.commit()

    print("Inserted record with calculated indicators and market cap:")
    print(json_data)

if __name__ == "__main__":
    consumer = KafkaConsumer(
        TopicsEnum.ETH_LOADER.value,
        bootstrap_servers=get_broker(),
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Running...")
    for data in consumer:
        digest(data.value)
