import pandas as pd


df = pd.read_csv("btc_data_hourly.csv")  


df['timestamp'] = pd.to_datetime(df['Timestamp'])


df['timestamp'] = df['timestamp'].astype('int64') // 10**9


df.to_csv("converted_file.csv", index=False)


print(df.head())
