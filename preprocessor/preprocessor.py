from kafka import KafkaConsumer
import json
import sys 
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.kafka_utils import TopicsEnum, get_broker, get_producer
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

def calculate_technical_indicators(df):
    
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df["high_price"] = pd.to_numeric(df["high_price"], errors="coerce")
    df["low_price"] = pd.to_numeric(df["low_price"], errors="coerce")
    
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



def nannull(val):
    if pd.isna(val):
        return None
    if isinstance(val, np.generic):
        return val.item()
    return val

def digest(new_record):
    print(new_record)
    global conn, cursor
    
    new_dt = datetime.datetime.fromtimestamp(new_record["timestamp"])
    new_date_key = int(new_dt.strftime('%Y%m%d%S'))
    new_record["date_key"] = new_date_key
    
    historical_query = """
        SELECT date_key, close_price, high_price, low_price, network_fees, open_price, 
               trading_volume, transaction_count, circulating_supply
        FROM fact_market_data
        WHERE pair_key = 1 AND asset_key = 1
        ORDER BY date_key
    """
    historical_df = pd.read_sql(historical_query, conn)
    
    
    if not historical_df.empty:
        historical_df["timestamp"] = historical_df["date_key"].astype(str).apply(
            lambda x: datetime.datetime.strptime(x, '%Y%m%d%S').timestamp()
        )
    else:
        historical_df["timestamp"] = pd.Series(dtype=float)
    
    
    new_df = pd.DataFrame([new_record])    
    combined_df = pd.concat([historical_df, new_df], ignore_index=True)
    combined_df = calculate_technical_indicators(combined_df)    
    updated_new_record = combined_df.iloc[-1]
    
    date_str = str(updated_new_record["date_key"])
    dt_date = datetime.datetime.strptime(date_str, '%Y%m%d%H')
    year, month, day, hour = dt_date.year, dt_date.month, dt_date.day, dt_date.hour
    quarter = (month - 1) // 3 + 1
    day_of_week = dt_date.weekday()
    
    check_query = """
        SELECT date_key FROM dim_date 
        WHERE year = %s AND month = %s AND day = %s AND hour = %s
    """
    cursor.execute(check_query, (year, month, day, hour))
    result = cursor.fetchone()
    
    if result is None:
        dim_date_key = int(dt_date.strftime('%Y%m%d%H'))
        insert_dim_query = """
            INSERT INTO dim_date(date_key, year, month, day, hour, quarter, day_of_week)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING date_key
        """
        cursor.execute(insert_dim_query, (dim_date_key, year, month, day, hour, quarter, day_of_week))
        date_key_new = cursor.fetchone()[0]
        conn.commit()
    else:
        date_key_new = result[0]
    
    circulating_supply = nannull(updated_new_record["circulating_supply"])
    close_price = nannull(updated_new_record["close_price"])
    market_cap = circulating_supply * close_price if circulating_supply is not None and close_price is not None else None

    trading_volume_val = float(updated_new_record["trading_volume"]) if isinstance(updated_new_record["trading_volume"], (str, np.generic)) else updated_new_record["trading_volume"]
    close_price_val = float(updated_new_record["close_price"]) if isinstance(updated_new_record["close_price"], (str, np.generic)) else updated_new_record["close_price"]

    trading_volume_USD = trading_volume_val * close_price_val if trading_volume_val is not None and close_price_val is not None else None
    json_data = {
        "network_fees": nannull(updated_new_record["network_fees"]),
        "close_price": nannull(updated_new_record["close_price"]),
        "open_price": nannull(updated_new_record["open_price"]),
        "high_price": nannull(updated_new_record["high_price"]),
        "low_price": nannull(updated_new_record["low_price"]),
        "trading_volume": trading_volume_USD,
        "transaction_count": nannull(updated_new_record["transaction_count"]),
        "circulating_supply": nannull(updated_new_record["circulating_supply"]),
        "market_cap": market_cap,
        "pair_key": 3,       
        "exchange_key": 3,  
        "asset_key": 1,     
        "date_key": date_key_new,
        "ema": nannull(updated_new_record["EMA_14"]),
        "rsi": nannull(updated_new_record["RSI_14"]),
        "sma": nannull(updated_new_record["SMI_14"])  
    }
    return json_data

if __name__ == "__main__":
    producer = get_producer()
    consumer = KafkaConsumer(
        TopicsEnum.ETH_POOL_DATA.value,
        bootstrap_servers=get_broker(),
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

    print("Running...")
    for data in consumer:
        result = digest(data.value)
        producer.send(TopicsEnum.ETH_LOADER.value, value=result)
