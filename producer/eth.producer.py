import pandas as pd
import json
import sys 
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from common.kafka_utils import get_producer, TopicsEnum
from record import getTick

import time

producer = get_producer()

last_time = 0
while True:
    last_time = time.time()
    data = getTick()
    print("I am running...")
    producer.send(TopicsEnum.ETH_POOL_DATA.value, value=data)
    producer.flush()

    break


    producer.close()


