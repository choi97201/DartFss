from tqdm import tqdm
import FinanceDataReader as fdr
from pykrx import stock
import datetime
import os
import time
from cjw_mysql import *
from utils import *


last = str(datetime.date.today() - datetime.timedelta(days=1)).replace('-', '')
res_path = f'C:/data/algo1'
start = str(datetime.datetime.strptime(last, '%Y%m%d') - datetime.timedelta(days=100))

main = Maria()
main.setMaria(db='ohlcv')
code_list = get_market_df(True)['종목코드'].tolist()

data1 = {}
data2 = {}
merged_data = {}

for idx in tqdm(range(len(code_list))):
    c = str(code_list[idx]).zfill(6)
    try:
        curr_df = stock.get_market_trading_value_by_date(start, last, c).reset_index(drop=False)[
            ['날짜', '기관합계', '기타법인', '개인', '외국인합계']]
        curr_df.columns = ['Date', 'Agency', 'Corp', 'Personal', 'Foreigner']
        curr_df['Date'] = curr_df['Date'].astype('str')
        curr_df['Date'] = [d.split(' ')[0] for d in list(curr_df['Date'])]
        if curr_df.shape[0] > 0 and curr_df.shape[1] == 5:
            data1[c] = curr_df

        curr_df = fdr.DataReader(c, start, last).reset_index(drop=False)
        curr_df['Date'] = curr_df['Date'].astype('str')
        curr_df['Date'] = [d.split(' ')[0] for d in list(curr_df['Date'])]
        cols = curr_df.columns[:-1]
        curr_df = curr_df[cols]
        if curr_df.shape[0] > 0:
            data2[c] = curr_df
        time.sleep(1)
    except:
        pass

try:
    sample_df = data1['005950']
except:
    sample_df = data1['005930']

wish_list = ['Agency', 'Corp', 'Personal', 'Foreigner']
for w in wish_list:
    merged_data[w] = pd.DataFrame()
    merged_data[w]['Date'] = sample_df['Date']

code_list = list(data1.keys())
for c in tqdm(range(len(code_list))):
    curr_df = data1[code_list[c]]
    for w in wish_list:
        try:
            merged_data[w] = pd.merge(merged_data[w], curr_df[['Date', w]].rename(columns={w: code_list[c]}), on=['Date'], how='outer').fillna(0).reset_index(drop=True)
        except:
            print(code_list[c])
            pass

for w in wish_list:
    merged_data[w] = merged_data[w].sort_values('Date').reset_index(drop=True)
    merged_data[w].to_csv(os.path.join(res_path, f'algo1_{w}.csv'.lower()), index=False, encoding='cp949')

try:
    sample_df = data2['005950']
except:
    sample_df = data2['005930']

wish_list = ['Open', 'High', 'Low', 'Close', 'Volume']
for w in wish_list:
    merged_data[w] = pd.DataFrame()
    merged_data[w]['Date'] = sample_df['Date']

curr_code_list = list(data2.keys())
for c in tqdm(range(len(curr_code_list))):
    curr_df = data2[curr_code_list[c]]
    for w in wish_list:
        merged_data[w] = pd.merge(merged_data[w], curr_df[['Date', w]].rename(columns={w: curr_code_list[c]}), on=['Date'], how='outer').fillna(0).reset_index(drop=True)

for w in wish_list:
    merged_data[w] = merged_data[w].sort_values('Date').reset_index(drop=True)
    merged_data[w].to_csv(os.path.join(res_path, f'algo1_{w}.csv'.lower()), index=False, encoding='cp949')


close_sma_df = pd.DataFrame()
close_sma_df['Date'] = merged_data['Close']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    close_sma_df[curr_code] = merged_data['Close'][curr_code].dropna().rolling(9, min_periods=9).mean().fillna(0)
close_sma_df.to_csv(os.path.join(res_path, 'algo1_close9sma.csv'), index=False, encoding='cp949')

close_sma_df = pd.DataFrame()
close_sma_df['Date'] = merged_data['Close']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    close_sma_df[curr_code] = merged_data['Close'][curr_code].dropna().rolling(10, min_periods=10).mean().fillna(0)
close_sma_df.to_csv(os.path.join(res_path, 'algo1_close10sma.csv'), index=False, encoding='cp949')

close_sma_df = pd.DataFrame()
close_sma_df['Date'] = merged_data['Close']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    close_sma_df[curr_code] = merged_data['Close'][curr_code].dropna().rolling(4, min_periods=4).mean().fillna(0)
close_sma_df.to_csv(os.path.join(res_path, 'algo1_close4sma.csv'), index=False, encoding='cp949')

fillna_df = pd.DataFrame()
fillna_df['Date'] = merged_data['Close']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    fillna_df[curr_code] = merged_data['Close'][curr_code].replace(to_replace=0, method='ffill')
fillna_df.to_csv(os.path.join(res_path, 'algo1_close_fillna.csv'), index=False, encoding='cp949')

fillna_df = pd.DataFrame()
fillna_df['Date'] = merged_data['Volume']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    fillna_df[curr_code] = merged_data['Volume'][curr_code].replace(to_replace=0, method='ffill')
fillna_df.to_csv(os.path.join(res_path, 'algo1_volume_fillna.csv'), index=False, encoding='cp949')

volume_sma_df = pd.DataFrame()
volume_sma_df['Date'] = merged_data['Volume']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    close_sma_df[curr_code] = merged_data['Volume'][curr_code].dropna().rolling(4, min_periods=4).mean().fillna(0)
close_sma_df.to_csv(os.path.join(res_path, 'algo1_volume4sma.csv'), index=False, encoding='cp949')

volume_sma_df = pd.DataFrame()
volume_sma_df['Date'] = merged_data['Volume']['Date']
for c in tqdm(range(len(curr_code_list))):
    curr_code = curr_code_list[c]
    close_sma_df[curr_code] = merged_data['Volume'][curr_code].dropna().rolling(5, min_periods=5).mean().fillna(0)
close_sma_df.to_csv(os.path.join(res_path, 'algo1_volume5sma.csv'), index=False, encoding='cp949')