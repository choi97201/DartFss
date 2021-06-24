import sys
from fnguide import Data
import pandas as pd
from tqdm import tqdm
import FinanceDataReader as fdr
from pykrx import stock
import datetime
import os
import numpy as np

res_path = 'C:/Users/choi97201/Desktop/cjw/data'

last = str(datetime.date.today()).replace('-', '')
main = Data()
code_df = main.getStockNameCodeDf()
code_list = code_df['종목코드'].tolist()
code_df = pd.read_excel(os.path.join(res_path, 'codes.xlsx'))
code_list = list(set(code_list + code_df['code'].tolist()))

data = {}

for idx in tqdm(range(len(code_list))):
    c = str(code_list[idx]).zfill(6)
    start = str(datetime.datetime.strptime(last, '%Y%m%d') - datetime.timedelta(days=20))
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


sma_df = pd.DataFrame()
sma_df['Date'] = df['Date']
for c in tqdm(range(len(code_list))):
    curr_code = code_list[c]
    sma_df[curr_code] = df[curr_code].dropna().rolling(5, min_periods=1).mean().fillna(0)
sma_df.to_csv(os.path.join(res_path, 'algo4_close5sma.csv'), index=False, encoding='cp949')

sep_df = df[code_list] / sma_df[code_list] * 100
sep_df['Date'] = df['Date']
sep_df = sep_df[['Date'] + code_list].fillna(0)
sep_df.to_csv(os.path.join(res_path, 'algo4_close5sep.csv'), index=False, encoding='cp949')


curr_sep_df = pd.DataFrame(sep_df.iloc[-1]).reset_index(drop=False)
curr_sep_df.columns = ['code', 'close5sep']
curr_sep_df = curr_sep_df.drop(0).reset_index(drop=True)
curr_sep_df = curr_sep_df.sort_values('close5sep')
curr_code_list = curr_sep_df[curr_sep_df['close5sep'] > 0]['code'].tolist()
curr_code_list = [curr_code for curr_code in curr_code_list if curr_code in corp_list]
first_rank_idx = int(np.ceil(len(curr_code_list) * 0.025))
last_rank_idx = int(np.ceil(len(curr_code_list) * 0.05))
curr_code_list = curr_code_list[first_rank_idx:last_rank_idx]