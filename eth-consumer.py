from kafka import KafkaConsumer
import json
from kafka_utils import TopicsEnum, get_broker
import pandas as pd
import numpy as np
import os
import psycopg2
import datetime


pg_host = os.environ.get("PG_HOST", "localhost")
pg_port = os.environ.get("PG_PORT", "5432")
pg_database = os.environ.get("PG_DATABASE", "comp430project")
pg_user = os.environ.get("PG_USER", "your_user")
pg_password = os.environ.get("PG_PASSWORD", "your_password")


try:
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()
    print("Connected to PostgreSQL database!")
except Exception as e:
    print("Error connecting to PostgreSQL:", e)


consumer = KafkaConsumer(
    TopicsEnum.ETH_DATASET.value,
    bootstrap_servers=get_broker(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def calculate_technical_indicators(df):
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    df["EMA_14"] = df["close_price"].ewm(span=14, adjust=False).mean()
    
    delta = df["close_price"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14, min_periods=14).mean()
    avg_loss = loss.rolling(window=14, min_periods=14).mean()
    rs = avg_gain / avg_loss
    df["RSI_14"] = 100 - (100 / (1 + rs))
    
    def calc_SMI(high, low, close, period=14, smooth1=3, smooth2=3):
        highest_high = high.rolling(window=period, min_periods=period).max()
        lowest_low = low.rolling(window=period, min_periods=period).min()
        mid = (highest_high + lowest_low) / 2
        diff = close - mid
        range_val = highest_high - lowest_low
        
        diff_smooth = diff.ewm(span=smooth1, adjust=False).mean().ewm(span=smooth2, adjust=False).mean()
        range_smooth = range_val.ewm(span=smooth1, adjust=False).mean().ewm(span=smooth2, adjust=False).mean()
        
        smi = 100 * (diff_smooth / (range_smooth / 2))
        return smi
    
    df["SMI_14"] = calc_SMI(df["high_price"], df["low_price"], df["close_price"], period=14, smooth1=3, smooth2=3)
    return df


for message in consumer:
    data = message.value
    
    
    df = pd.DataFrame(data)
    
    df_with_indicators = calculate_technical_indicators(df)
    
    print(df_with_indicators[["timestamp", "close_price", "EMA_14", "RSI_14", "SMI_14"]])
    
    for i, row in df.iterrows():
        
        record_timestamp = row["timestamp"]
        dt = datetime.datetime.fromtimestamp(record_timestamp)
        year = dt.year
        month = dt.month
        day = dt.day
        hour = dt.hour
        quarter = (month - 1) // 3 + 1
        day_of_week = dt.weekday()
        
        
        check_query = """
            SELECT id FROM dim_date 
            WHERE year = %s AND month = %s AND day = %s AND hour = %s
        """
        cursor.execute(check_query, (year, month, day, hour))
        result = cursor.fetchone()
        
        if result is None:
            
            insert_dim_query = """
                INSERT INTO dim_date(year, month, day, hour, quarter, day_of_week)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """
            cursor.execute(insert_dim_query, (year, month, day, hour, quarter, day_of_week))
            date_key = cursor.fetchone()[0]
            conn.commit()
        else:
            date_key = result[0]
        
        
        ema = df_with_indicators["EMA_14"].iloc[i]
        rsi = df_with_indicators["RSI_14"].iloc[i]
        smi = df_with_indicators["SMI_14"].iloc[i]
        
        
        json_data = {
            "network_fees": row["network_fees"],
            "close_price": row["close_price"],
            "open_price": row["open_price"],
            "high_price": row["high_price"],
            "low_price": row["low_price"],
            "trading_volume": row["trading_volume"],
            "transaction_count": row["transaction_count"],
            "timestamp": datetime.datetime.fromtimestamp(row["timestamp"]),  
            "cumulative_supply": row["cumulative_supply"],
            "pair_key": 3,       
            "exchange_key": 3,  
            "asset_key": 1,     
            "date_key": date_key,
            "ema": ema,
            "rsi": rsi,
            "sma": smi
        }
        
        
        insert_fact_query = """
        INSERT INTO fact_market_data
        (network_fees, close_price, open_price, high_price, low_price, trading_volume, transaction_count, timestamp, cumulative_supply, pair_key, exchange_key, asset_key, date_key, ema, rsi, smi)
        VALUES (%(network_fees)s, %(close_price)s, %(open_price)s, %(high_price)s, %(low_price)s, %(trading_volume)s, %(transaction_count)s, %(timestamp)s, %(cumulative_supply)s, %(pair_key)s, %(exchange_key)s, %(asset_key)s, %(date_key)s, %(ema)s, %(rsi)s, %(smi)s)
        """
        cursor.execute(insert_fact_query, json_data)
    
    
    conn.commit() 