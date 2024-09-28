import datetime
import pandas as pd

date1 = pd.to_datetime(datetime.datetime.now(datetime.UTC)).strftime(format='%Y-%m-%d %H:%M')

data = list()
data.append(str(date1))

df = pd.DataFrame(data, columns=['date'])

print(df)
