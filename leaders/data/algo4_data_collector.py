from tqdm import tqdm
import FinanceDataReader as fdr
import datetime
import os
from cjw_mysql import *
from utils import *
import numpy as np

res_path = '../'

last = str(datetime.date.today()).replace('-', '')
start = str(datetime.datetime.strptime(last, '%Y%m%d') - datetime.timedelta(days=20))

main = Maria()
main.setMaria(db='ohlcv')
stock_info = main.mariaShowData('stock_info')
stock_info.to_csv(os.path.join(res_path, 'algo4_stock_info.csv'), index=False, encoding='cp949')
code_list = get_market_df(True)['종목코드'].tolist()

data = {}

for idx in tqdm(range(len(code_list))):
    c = str(code_list[idx]).zfill(6)
    data[c] = fdr.DataReader(c, start, last).reset_index(drop=False)
    data[c]['Date'] = data[c]['Date'].astype('str')
    data[c]['Date'] = [d.split(' ')[0] for d in list(data[c]['Date'])]
    cols = data[c].columns[:-1]
    data[c] = data[c][cols]

sample_df = data['005950']

df = pd.DataFrame()
df['Date'] = sample_df['Date']
for c in tqdm(range(len(code_list))):
    curr_df = data[code_list[c]]
    df = pd.merge(df, curr_df[['Date', 'Close']].rename(columns={'Close': code_list[c]}), on=['Date'], how='outer').fillna(0).reset_index(drop=True)
df = df.sort_values('Date').reset_index(drop=True)
df.to_csv(os.path.join(res_path, 'algo4_close.csv'), index=False, encoding='cp949')

cols = list(df.columns)[1:]
fillna_close = pd.DataFrame()
fillna_close['Date'] = df['Date']
for curr_code in tqdm(cols):
    fillna_close[curr_code] = df[curr_code].replace(to_replace=0, method='ffill')
fillna_close = fillna_close.fillna(0)
fillna_close.to_csv(os.path.join(res_path, 'algo4_close_fillna.csv'), index=False, encoding='cp949')

change_df = pd.DataFrame()
change_df['Date'] = df['Date']
for curr_code in tqdm(cols):
    change_df[curr_code] = (fillna_close[curr_code] / fillna_close[curr_code].shift(1) - 1) * 100
change_df = change_df.fillna(0).replace(np.inf, 0)
change_df.to_csv(os.path.join(res_path, 'algo4_change_close2close.csv'), index=False, encoding='cp949')