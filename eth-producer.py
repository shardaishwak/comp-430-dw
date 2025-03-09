import pandas as pd
import json
from kafka_utils import get_producer, TopicsEnum
from record import fetch

import time

producer = get_producer()

last_time = 0
while True:
    if time.time() - last_time > 3600:
        last_time = time.time()
        # Fetch the data
        # Send the data to Kafka
        data = fetch()
        producer.send(TopicsEnum.ETH_POOL_DATA.value, value=json.dumps(data).encode())


producer.flush()
producer.close()