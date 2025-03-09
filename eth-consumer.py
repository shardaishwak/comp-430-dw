from kafka import KafkaConsumer
import json
from kafka_utils import TopicsEnum, get_broker
import pandas as pd
import numpy as np

# Oracle connection details (if needed)
# oracle_host = os.environ.get("ORACLE_HOST", "localhost")
# oracle_port = os.environ.get("ORACLE_PORT", "1521")
# oracle_service_name = os.environ.get("ORACLE_SERVICE_NAME", "xe") 
# oracle_user = os.environ.get("ORACLE_USER", "your_user")
# oracle_password = os.environ.get("ORACLE_PASSWORD", "your_password")

columns = [
    "network_fees", "close_price", "open_price", "high_price", "trading_volume", 
    "transaction_count", "low_price", "timestamp", "cumulative_supply"
]

consumer = KafkaConsumer(
    TopicsEnum.ETH_DATASET.value,
    bootstrap_servers=get_broker(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Oracle connection setup (if needed)
# dsn_tns = cx_Oracle.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
# conn = cx_Oracle.connect(user=oracle_user, password=oracle_password, dsn=dsn_tns)
# cursor = conn.cursor()

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

    # In production, you would execute an Oracle query like:
    # get_14_records_query = "SELECT * FROM (SELECT * FROM ETH_DATA ORDER BY TIMESTAMP DESC) WHERE ROWNUM <= 14"
    # cursor.execute(get_14_records_query)
    # records = cursor.fetchall()
    # Then convert those records into a DataFrame with the defined columns.
    
    simulated_data = {
        "network_fees": np.random.uniform(0.1, 1.0, 14).tolist(),
        "close_price": np.random.uniform(1800, 1900, 14).tolist(),
        "open_price": np.random.uniform(1800, 1900, 14).tolist(),
        "high_price": np.random.uniform(1900, 2000, 14).tolist(),
        "trading_volume": np.random.uniform(100, 500, 14).tolist(),
        "transaction_count": np.random.randint(50, 100, 14).tolist(),
        "low_price": np.random.uniform(1700, 1800, 14).tolist(),
        "timestamp": pd.date_range(end=pd.Timestamp.now(), periods=14, freq="H").tolist(),
        "cumulative_supply": np.random.uniform(110000, 120000, 14).tolist()
    }
    
    df = pd.DataFrame(simulated_data)
    
    df_with_indicators = calculate_technical_indicators(df)
    
    print(df_with_indicators[["timestamp", "close_price", "EMA_14", "RSI_14", "SMI_14"]])
    
    break
