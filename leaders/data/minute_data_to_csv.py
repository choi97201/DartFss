import datetime
from tqdm import tqdm
import time
import os
import sys
from cjw_mysql import *

maria = Maria()
maria.setMaria(host='localhost', user='root', password='sa1234', db='ohlcv', charset='utf8')

table_list = maria.mariaShowTables()['Tables_in_ohlcv'].tolist()[:-1]
table_list = [curr_table for curr_table in table_list if 'min' in curr_table and 'update' not in curr_table and 'q' not in curr_table]
for curr_table in table_list:
    print(curr_table)
def play(start_month, last_month):
    curr_month = start_month
    std_table_name = 'a005930_min'
    data = {}
    wish_list = ['open', 'high', 'low', 'close', 'volume']
    data_path = 'C:/data/minute_month/20210507'
    while True:
        print(curr_month)
        time.sleep(1)
        sql = f'select * from {std_table_name} where date like "{curr_month}%"'
        std_table = maria.mariaShowData(std_table_name, sql)['date'].astype('str').tolist()

        for curr_wish in wish_list:
            data[curr_wish] = pd.DataFrame(std_table, columns=['date'])
        for curr_table in tqdm(table_list):
            curr_code = curr_table.replace('a', '').replace('_min', '')
            sql = f'select * from {curr_table} where date like "{curr_month}%"'
            curr_min_table = maria.mariaShowData(curr_table, sql)
            curr_min_table['date'] = curr_min_table['date'].astype('str')
            for curr_wish in wish_list:
                data[curr_wish] = pd.merge(data[curr_wish],
                                           curr_min_table.rename(columns={curr_wish: curr_code})[['date', curr_code]],
                                           on=['date'], how='outer').reset_index(drop=True)

        for curr_wish in wish_list:
            data[curr_wish].fillna(0).sort_values('date').reset_index(drop=True).to_csv(os.path.join(data_path,
                                                                     f'minute_{curr_month}_{curr_wish}.csv'), index=False)

        curr_month = str(datetime.datetime.strptime(curr_month, '%Y-%m') + datetime.timedelta(days=31)).split(' ')[0]
        curr_month = curr_month[:7]

        if curr_month == last_month:
            break

start_month = sys.argv[1]
last_month = sys.argv[2]
play(start_month, last_month)

