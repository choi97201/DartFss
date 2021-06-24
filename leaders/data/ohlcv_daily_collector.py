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
last = '20210507'
main = Data()
main.setMaria(host='localhost', user='root', password='sa1234', db='ohlcv', charset='utf8', port=3306)
table_list = main.mariaShowTables()
table_list = list(table_list[table_list.columns[0]])

code_list = main.mariaShowData('stock_info')['code'].tolist()

create_cols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Value', 'Capital', 'Stocks', 'Agency', 'Corp', 'Personal', 'Foreigner']
dtypes = ['varchar(20)', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']


for idx in tqdm(range(len(code_list))):
    c = code_list[idx]
    tablename = 'a' + c + '_day'
    start = '19500101'
    if not tablename in table_list:
        main.mariaCreateTable(tablename=tablename, columns=create_cols, columns_type=dtypes)
    else:
        main.mariaSql(f'drop table {tablename}')
        main.mariaCreateTable(tablename=tablename, columns=create_cols, columns_type=dtypes)
    if str(datetime.date.today()) == str(start).split(' ')[0]:
        if int(str(datetime.datetime.today()).split(' ')[1].split(':')[0]) < 18:
            continue
    df = fdr.DataReader(c, start, last).reset_index(drop=False)
    df['Date'] = df['Date'].astype('str')
    df['Date'] = [d.split(' ')[0] for d in list(df['Date'])]
    cols = df.columns[:-1]
    df = df[cols]
    cap_df = stock.get_market_cap_by_date(start, last, c).reset_index(drop=False)[['날짜', '거래대금', '시가총액', '상장주식수']]
    cap_df.columns = ['Date', 'Value', 'Capital', 'Stocks']
    cap_df['Date'] = cap_df['Date'].astype('str')
    df = pd.merge(df, cap_df, on=['Date']).reset_index(drop=True)
    for_df = stock.get_market_trading_value_by_date(start, last, c).reset_index(drop=False)[['날짜', '기관합계', '기타법인', '개인', '외국인합계']]
    for_df.columns = ['Date', 'Agency', 'Corp', 'Personal', 'Foreigner']
    for_df['Date'] = for_df['Date'].astype('str')
    df = pd.merge(df, for_df, on=['Date']).reset_index(drop=True)
    cols = df.columns
    dtypes = ['varchar(20)'] + (['FLOAT'] * (len(cols)-1))
    for i in range(df.shape[0]):
        main.mariaInsertData(tablename, tuple(df.iloc[i]))
    main.mariaCommitDB()

wish_list = ['Close', 'Open', 'High', 'Low', 'Volume', 'Value', 'Capital', 'Stocks', 'Agency', 'Corp', 'Personal', 'Foreigner']

table_list = main.mariaShowTables()['Tables_in_ohlcv'].tolist()
code_list = [table.replace('_day', '').replace('a', '') for table in table_list if 'day' in table]
code_list = [code for code in code_list if code[-1] == '0']

sample_df = main.mariaShowData('a005950_day')
for w in wish_list:
    df = pd.DataFrame()
    df['Date'] = sample_df['Date']
    for c in tqdm(range(len(code_list))):
        tablename = 'a' + code_list[c] + '_day'
        curr_df = main.mariaShowData(tablename)
        df = pd.merge(df, curr_df[['Date', w]].rename(columns={w: code_list[c]}), on=['Date'], how='outer').fillna(0).reset_index(drop=True)
    df = df.sort_values('Date').reset_index(drop=True)
    df.to_csv(os.path.join(res_path, 'day_{}.csv'.format(w).lower()), index=False, encoding='utf-8')

etf_list = ['114800', '122630', '069500', '252670', '233740', '251340', '091160', '229200']
for e in etf_list:
    df = stock.get_etf_ohlcv_by_date("20020101", '20210507', e).reset_index(drop=False)
    df = df[['날짜', 'NAV', '시가', '고가', '저가', '종가', '거래량']]
    df.columns = ['date', 'NAV', 'open', 'high', 'low', 'close', 'volume']
    df.to_csv(os.path.join(res_path, 'etf_{}.csv'.format(e)), index=False)


df = pd.read_csv(os.path.join(res_path, 'day_close.csv'))

cols = list(df.columns)[1:]

sma5_df = pd.DataFrame()
sma10_df = pd.DataFrame()
sma20_df = pd.DataFrame()
sma60_df = pd.DataFrame()
sma120_df = pd.DataFrame()

sma5_df['Date'] = df['Date']
sma10_df['Date'] = df['Date']
sma20_df['Date'] = df['Date']
sma60_df['Date'] = df['Date']
sma120_df['Date'] = df['Date']

for curr_code in tqdm(cols):
    sma5_df[curr_code] = df[curr_code].replace(0, np.nan).dropna().rolling(5, min_periods=5).mean().fillna(0)
    sma10_df[curr_code] = df[curr_code].replace(0, np.nan).dropna().rolling(10, min_periods=10).mean().fillna(0)
    sma20_df[curr_code] = df[curr_code].replace(0, np.nan).dropna().rolling(20, min_periods=20).mean().fillna(0)
    sma60_df[curr_code] = df[curr_code].replace(0, np.nan).dropna().rolling(60, min_periods=60).mean().fillna(0)
    sma120_df[curr_code] = df[curr_code].replace(0, np.nan).dropna().rolling(120, min_periods=120).mean().fillna(0)

sma5_df = sma5_df.fillna(0)
sma10_df = sma10_df.fillna(0)
sma20_df = sma20_df.fillna(0)
sma60_df = sma60_df.fillna(0)
sma120_df = sma120_df.fillna(0)

sma5_df.to_csv(os.path.join(res_path, 'day_close5sma.csv'), index=False, encoding='cp949')
sma10_df.to_csv(os.path.join(res_path, 'day_close10sma.csv'), index=False, encoding='cp949')
sma20_df.to_csv(os.path.join(res_path, 'day_close20sma.csv'), index=False, encoding='cp949')
sma60_df.to_csv(os.path.join(res_path, 'day_close60sma.csv'), index=False, encoding='cp949')
sma120_df.to_csv(os.path.join(res_path, 'day_close120sma.csv'), index=False, encoding='cp949')


sep5_df = df[cols] / sma5_df[cols]
sep5_df['Date'] = df['Date']
sep5_df = sep5_df[['Date'] + cols].fillna(0)

sep10_df = df[cols] / sma10_df[cols]
sep10_df['Date'] = df['Date']
sep10_df = sep10_df[['Date'] + cols].fillna(0)

sep20_df = df[cols] / sma20_df[cols]
sep20_df['Date'] = df['Date']
sep20_df = sep20_df[['Date'] + cols].fillna(0)

sep60_df = df[cols] / sma60_df[cols]
sep60_df['Date'] = df['Date']
sep60_df = sep60_df[['Date'] + cols].fillna(0)

sep120_df = df[cols] / sma120_df[cols]
sep120_df['Date'] = df['Date']
sep120_df = sep120_df[['Date'] + cols].fillna(0)

sep5_df.to_csv(os.path.join(res_path, 'day_close5sep.csv'), index=False, encoding='cp949')
sep10_df.to_csv(os.path.join(res_path, 'day_close10sep.csv'), index=False, encoding='cp949')
sep20_df.to_csv(os.path.join(res_path, 'day_close20sep.csv'), index=False, encoding='cp949')
sep60_df.to_csv(os.path.join(res_path, 'day_close60sep.csv'), index=False, encoding='cp949')
sep120_df.to_csv(os.path.join(res_path, 'day_close120sep.csv'), index=False, encoding='cp949')

cols = list(df.columns)[1:]
fillna_close = pd.DataFrame()
fillna_close['Date'] = df['Date']
for curr_code in tqdm(cols):
    fillna_close[curr_code] = df[curr_code].replace(to_replace=0, method='ffill')
fillna_close = fillna_close.fillna(0)
fillna_close.to_csv(os.path.join(res_path, 'day_close_fillna.csv'), index=False, encoding='cp949')

df = pd.read_csv(os.path.join(res_path, 'day_volume.csv'))
cols = list(df.columns)[1:]
fillna_volume = pd.DataFrame()
fillna_volume['Date'] = df['Date']
for curr_code in tqdm(cols):
    fillna_volume[curr_code] = df[curr_code].replace(to_replace=0, method='ffill')
fillna_volume = fillna_volume.fillna(0)
fillna_volume.to_csv(os.path.join(res_path, 'day_volume_fillna.csv'), index=False, encoding='cp949')