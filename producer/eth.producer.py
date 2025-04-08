import os
import sys
import time
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.kafka_utils import get_producer, TopicsEnum, get_connection
from record import getTick

producer = get_producer()

CHECK_INTERVAL_SECONDS = 5

def parse_date_key(date_key: int) -> datetime:
    """Convert an int like 2025040516 into a datetime object."""
    return datetime.strptime(str(date_key), "%Y%m%d%H")

def get_latest_record():
    query = """
        SELECT date_key
        FROM fact_market_data
        WHERE pair_key = 1 AND asset_key = 1
        ORDER BY date_key DESC
        LIMIT 1;
    """
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchone()
    except Exception as e:
        print(f"[DB ERROR] {e}")
        return None

def main_loop():
    latest_record = get_latest_record()
    if latest_record:
        last_date = parse_date_key("2024053123")
        next_date_to_fetch = last_date + timedelta(hours=1)
        print(f"[INIT] Latest record date: {last_date}. Setting next fetch to {next_date_to_fetch}.")
    else:
        now = datetime.utcnow()
        
        next_date_to_fetch = now.replace(minute=0, second=0, microsecond=0)
        print(f"[INIT] No records found. Setting next fetch to {next_date_to_fetch}.")

    while True:
        now = datetime.utcnow()
        
        if next_date_to_fetch > now:
            print(f"[INFO] Next fetch date {next_date_to_fetch} is in the future (current time: {now}). Waiting...")
            time.sleep(CHECK_INTERVAL_SECONDS)
            continue

        print(f"[INFO] Fetching tick for {next_date_to_fetch} (current time: {now}).")
        
        data = getTick(next_date_to_fetch)
        print(f"[PRODUCER] Sending data to Kafka for date: {next_date_to_fetch}")
        producer.send(TopicsEnum.ETH_POOL_DATA.value, value=data)
        producer.flush()

        
        next_date_to_fetch += timedelta(hours=1)
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main_loop()
