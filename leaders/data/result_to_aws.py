from cjw_mysql import Maria
import pandas as pd
from fnguide import *
import datetime

main = Data()
# main.setMaria()
# df = main.mariaShowData('data_value_score_2021_03')
# df = df[['code', 'profit_score', 'grow_score', 'stable_score', 'market_score', 'total_score', '주당가치분석']].sort_values('total_score', ascending=False)
# df.columns = list(df.columns[:-1]) + ['min_band_price']
# df['min_band_price'] = df['min_band_price'].astype('int')
# df.to_csv('new_df.csv', index=False)

new_df = pd.read_csv('new_df.csv')
new_df['code'] = [str(code).zfill(6) for code in new_df['code'].tolist()]
curr_date = str(datetime.datetime.now()).split(' ')[0]
new_df['update_date'] = [curr_date] * new_df.shape[0]
print(new_df)
main.setMaria('15.165.29.213', 'lt_user', 'De4IjOY32e7o', 'leaderstrading', port=3306)
for i in range(new_df.shape[0]):
    main.mariaInsertData('value_score_2020_09', tuple(new_df.iloc[i]))
main.mariaCommitDB()
