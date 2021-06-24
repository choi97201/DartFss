import pandas as pd
import os
from cjw_mysql import *
import tqdm
import numpy as np

maria = Maria()
maria.setMaria(host='localhost', user='root', password='sa1234', db='ohlcv', charset='utf8')
table_list = maria.mariaShowTables()['Tables_in_ohlcv'].tolist()

coded_df = pd.read_csv

min_data_path = 'C:/data/krx'
min_file_list = os.listdir(min_data_path)
rename_param = {'종목코드': 'code', '시가': 'open', '고가': 'high', '저가': 'low', '종가': 'close', '거래량': 'volume'}
dtype_param = {'거래일자': np.str, '시각': np.str, '종목코드': np.str, '시가': np.float_,
               '고가': np.float_, '저가': np.float_, '종가': np.float_, '거래량': np.float_}

count = 1
for file in min_file_list:
    print('[{}][{}/{}]'.format(file, count, len(min_file_list)))
    curr_df = pd.read_csv(os.path.join(min_data_path, file), encoding='cp949', dtype=dtype_param)[[
        '거래일자', '시각', '종목코드', '시가', '고가', '저가', '종가', '거래량']
    ].dropna().reset_index(drop=True)
    curr_df = curr_df.rename(columns=rename_param)
    curr_df['거래일자'] = curr_df['거래일자'].astype('str')
    print('시각을 변환합니다...')
    curr_df['시각'] = [str(curr_time).zfill(4) for curr_time in tqdm.tqdm(curr_df['시각'].tolist())]
    curr_df['시각'] = ['{}:{}:00'.format(curr_time[:2], curr_time[2:]) for curr_time in tqdm.tqdm(curr_df['시각'].tolist())]
    curr_code_list = list(set(curr_df['code'].tolist()))
    for code in tqdm.tqdm(curr_code_list):
        curr_code_df = curr_df[curr_df['code'] == code]
        curr_code_df[['date']] = ['{}-{}-{} {}'.format(curr_code_df['거래일자'].iloc[i][:4], curr_code_df ['거래일자'].iloc[i][4:6], curr_code_df ['거래일자'].iloc[i][6:], curr_code_df['시각'].iloc[i]) for i in range(curr_code_df.shape[0])]
        curr_code_df = curr_code_df[['date', 'open', 'high', 'low', 'close', 'volume']]
        tablename = 'a' + str(code).zfill(6) + '_min' if len(code) <= 6 else code.lower() + '_min'
        if not tablename.lower() in table_list:
            cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            dtypes = ['datetime', 'int', 'int', 'int', 'int', 'int']
            maria.mariaCreateTable(tablename, cols, dtypes)
            table_list.append(tablename)
            orgin_date_list = []
        else:
            orgin_date_list = maria.mariaShowData(tablename)['date'].astype('str').tolist()
        for idx in range(curr_code_df.shape[0]):
            if curr_code_df['date'].iloc[idx] in orgin_date_list:
                continue
            maria.mariaInsertData(tablename, tuple(curr_code_df.iloc[idx]))
        maria.mariaCommitDB()
    count += 1