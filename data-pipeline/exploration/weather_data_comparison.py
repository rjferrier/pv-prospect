from pathlib import Path

import pandas as pd

df = pd.read_csv(Path('../datasets/data-1/pvoutput_89665.csv'))
df['datetime'] = pd.to_datetime(df['date'].astype(str) + 'T' + df['time'])
df = df[['datetime', 'power', 'average', 'temperature']]
df.head()
