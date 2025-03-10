import requests
import pandas as pd
from pprint import pprint

API_KEY = "a0006cc125ffe8010989fb8ccff8712b"
GRAPHQL_URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV"
GRAPHQL_URL_2 = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/G1pPbbMjwCnFiyMherq8wqfMusZDriLMqvGBHLr2wS34"




def getTick():
  query = """
  query MyQuery {
    poolHourDatas(
      where: {pool: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"}
      orderBy: periodStartUnix
      orderDirection: desc
      first: 1
    ) {
      periodStartUnix
      open
      high
      low
      close
      volumeUSD
      liquidity
      feesUSD
      volumeToken0
      volumeToken1
      txCount
      tick
      feesUSD
    }
  }
  """

  query_2 = """
  query MyQuery {
    poolHourlySnapshots(first: 2, orderBy: timestamp, orderDirection: desc) {
      netCumulativeVolume
      hour
      timestamp
      }
  }
  """

  response = requests.post(GRAPHQL_URL, json={'query': query})

  data = response.json()
  d = data['data']['poolHourDatas']

  df = pd.DataFrame(d)
  print(df)
  data_2 = requests.post(GRAPHQL_URL_2, json={'query': query_2}).json()
  d_2 = data_2['data']['poolHourlySnapshots']
  df_2 = pd.DataFrame(d_2)
  print(df_2)

  response = {
    "network_fees": d[0]['feesUSD'],
    "close_price": d[0]['close'],
    "open_price": d[0]['open'],
    "high_price": d[0]['high'],
    "low_price": d[0]['low'],
    "trading_volume": d[0]['volumeToken1'],
    "transaction_count": d[0]['txCount'],
    "timestamp": d[0]['periodStartUnix'],
    "cumulative_supply": float(d_2[0]['netCumulativeVolume']) if float(d_2[0]['netCumulativeVolume']) > 0 else float(d_2[1]['netCumulativeVolume']),
  }

  return response


pprint(getTick())