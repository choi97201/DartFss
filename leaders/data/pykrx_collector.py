import sys
from fnguide import Data
import pandas as pd
from tqdm import tqdm
# import FinanceDataReader as fdr
from pykrx import stock
import numpy as np
import datetime
import os
import time

tickers_kospi = stock.get_market_ticker_list("20210514", market="KOSPI")
tickers_kosdaq = stock.get_market_ticker_list("20210514", market="KOSDAQ")
tickers = tickers_kospi + tickers_kosdaq
print(tickers)

df = stock.get_market_ohlcv_by_date("20180201", "20210514", "005930")
print(df)

date_list = df.index.to_list()
for n in tqdm(range(len(date_list))):

    if n < 70:
        continue

    date = str(date_list[n])[0:10]
    date = date.replace('-', '')
    # date = "20210105"

    # 1. 거래대금 및 거래량
    df_volume_temp = stock.get_market_ohlcv_by_ticker(date, market="ALL")
    df_volume_temp = df_volume_temp.sort_values(by=['티커'], axis=0, na_position='last', ascending=True).reset_index()
    print(df_volume_temp)

    # 거래대금
    df_fund = df_volume_temp[['티커', '거래대금']]
    df_fund = df_fund.sort_values(by=['거래대금'], axis=0, na_position='last', ascending=False)
    df_fund = df_fund.reset_index(drop=True)
    print(df_fund)
    df_fund['fund_rank'] = df_fund.index
    print(df_fund)
    df_fund = df_fund.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_fund)
    df_fund.reset_index(drop=False, inplace=True)
    print(df_fund)
    df_fund.set_index('티커')
    print(df_fund)

    # 거래량
    df_volume = df_volume_temp[['티커', '거래량']]
    print(df_volume)
    df_volume = df_volume.sort_values(by=['거래량'], axis=0, na_position='last', ascending=False)
    print(df_volume)
    df_volume = df_volume.reset_index(drop=True)
    print(df_volume)
    df_volume['volume_rank'] = df_volume.index
    print(df_volume)
    df_volume = df_volume.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_volume)
    df_volume.reset_index(drop=False, inplace=True)
    print(df_volume)
    df_volume.set_index('티커')
    print(df_volume)

    date_start = str(date_list[n - 70])[0:10]
    date_start = date_start.replace('-', '')
    result_list = []
    for i in range(len(tickers)):

        # if i > 20:
        #     continue

        stock_id = tickers[i]
        df_ohlcv = stock.get_market_ohlcv_by_date(date_start, date, stock_id)
        # print(df_ohlcv)
        temp = len(df_ohlcv)

        if temp == 0:
            continue

        if len(df_ohlcv) - 28 < 0:
            continue

        # 5SMA
        # print(df_ohlcv.iloc[len(df_ohlcv)-5:len(df_ohlcv)]['종가'])
        sma5 = df_ohlcv.iloc[len(df_ohlcv) - 5:len(df_ohlcv)]['종가'].sum() / 5.0
        sma5_rate = df_ohlcv.iloc[-1]['종가'] / sma5
        # print(sma5_rate)

        # 20SMA
        # print(df_ohlcv.iloc[len(df_ohlcv)-20:len(df_ohlcv)]['종가'])
        sma20 = df_ohlcv.iloc[len(df_ohlcv) - 20:len(df_ohlcv)]['종가'].sum() / 20.0
        sma20_rate = df_ohlcv.iloc[-1]['종가'] / sma20
        # print(sma20_rate)

        # 60SMA
        # print(df_ohlcv.iloc[len(df_ohlcv)-60:len(df_ohlcv)]['종가'])
        sma60 = df_ohlcv.iloc[len(df_ohlcv) - 60:len(df_ohlcv)]['종가'].sum() / 60.0
        sma60_rate = df_ohlcv.iloc[-1]['종가'] / sma60
        # print(sma60_rate)

        # 최근 4주 상승률
        # print(df_ohlcv.iloc[len(df_ohlcv)-28:len(df_ohlcv)]['종가'])
        past_price = df_ohlcv.iloc[len(df_ohlcv) - 28]['종가']
        inc_rate = df_ohlcv.iloc[-1]['종가'] / past_price
        # print(inc_rate)

        result_list.append([stock_id, sma5_rate, sma20_rate, sma60_rate, inc_rate])

    # 5SMA
    df_5sma = pd.DataFrame()
    df_5sma['티커'] = [result_list[i][0] for i in range(len(result_list))]
    df_5sma.set_index('티커')
    df_5sma['sma5_rate'] = [result_list[i][1] for i in range(len(result_list))]

    df_5sma = df_5sma.sort_values(by=['sma5_rate'], axis=0, na_position='last', ascending=True)
    print(df_5sma)
    df_5sma = df_5sma.reset_index(drop=True)
    print(df_5sma)
    df_5sma['sma5_rank'] = df_5sma.index
    print(df_5sma)
    df_5sma = df_5sma.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_5sma)
    df_5sma = df_5sma.drop(columns=["sma5_rate"])
    df_5sma.set_index('티커')
    print(df_5sma)

    # 20SMA
    df_20sma = pd.DataFrame()
    df_20sma['티커'] = [result_list[i][0] for i in range(len(result_list))]
    df_20sma.set_index('티커')
    df_20sma['sma20_rate'] = [result_list[i][2] for i in range(len(result_list))]

    df_20sma = df_20sma.sort_values(by=['sma20_rate'], axis=0, na_position='last', ascending=True)
    print(df_20sma)
    df_20sma = df_20sma.reset_index(drop=True)
    print(df_20sma)
    df_20sma['sma20_rank'] = df_20sma.index
    print(df_20sma)
    df_20sma = df_20sma.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_20sma)
    df_20sma = df_20sma.drop(columns=["sma20_rate"])
    df_20sma.set_index('티커')
    print(df_20sma)

    # 60SMA
    df_60sma = pd.DataFrame()
    df_60sma['티커'] = [result_list[i][0] for i in range(len(result_list))]
    df_60sma.set_index('티커')
    df_60sma['sma60_rate'] = [result_list[i][3] for i in range(len(result_list))]

    df_60sma = df_60sma.sort_values(by=['sma60_rate'], axis=0, na_position='last', ascending=True)
    print(df_60sma)
    df_60sma = df_60sma.reset_index(drop=True)
    print(df_60sma)
    df_60sma['sma60_rank'] = df_60sma.index
    print(df_60sma)
    df_60sma = df_60sma.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_60sma)
    df_60sma = df_60sma.drop(columns=["sma60_rate"])
    df_60sma.set_index('티커')
    print(df_60sma)

    # 최근 상승률
    df_inc_rate = pd.DataFrame()
    df_inc_rate['티커'] = [result_list[i][0] for i in range(len(result_list))]
    df_inc_rate.set_index('티커')
    df_inc_rate['inc_rate'] = [result_list[i][4] for i in range(len(result_list))]

    df_inc_rate = df_inc_rate.sort_values(by=['inc_rate'], axis=0, na_position='last', ascending=True)
    print(df_inc_rate)
    df_inc_rate = df_inc_rate.reset_index(drop=True)
    print(df_inc_rate)
    df_inc_rate['inc_rank'] = df_inc_rate.index
    print(df_inc_rate)
    df_inc_rate = df_inc_rate.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_inc_rate)
    df_inc_rate = df_inc_rate.drop(columns=["inc_rate"])
    df_inc_rate.set_index('티커')
    print(df_inc_rate)

    #
    df_fundamental = stock.get_market_fundamental_by_ticker("20210105", market="ALL")
    df_fundamental = df_fundamental.replace(0, np.NaN)
    df_fundamental = df_fundamental.sort_values(by=['티커'], axis=0, na_position='last', ascending=True).reset_index()
    print(df_fundamental)

    # 1. PER(Price Earning Ratio) 낮을 수록 좋음
    df_per = df_fundamental[['티커', 'PER']]
    print(df_per)
    df_per = df_per.sort_values(by=['PER'], axis=0, na_position='last', ascending=True)
    print(df_per)
    df_per = df_per.reset_index(drop=True)
    print(df_per)
    df_per['per_rank'] = df_per.index
    print(df_per)

    df_per.loc[df_per["PER"].isnull(), "per_rank"] = np.NaN
    print(df_per)

    df_per = df_per.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_per)
    df_per.reset_index(drop=False, inplace=True)
    print(df_per)
    df_per.set_index('티커')
    print(df_per)

    # 2. PBR(Price-To-Book Ratio) 낮을 수록 좋음
    df_pbr = df_fundamental[['티커', 'PBR']]
    print(df_pbr)
    df_pbr = df_pbr.sort_values(by=['PBR'], axis=0, na_position='last', ascending=True)
    print(df_pbr)
    df_pbr = df_pbr.reset_index(drop=True)
    print(df_pbr)
    df_pbr['pbr_rank'] = df_pbr.index
    print(df_pbr)
    df_pbr.loc[df_pbr["PBR"].isnull(), "pbr_rank"] = np.NaN
    print(df_pbr)
    df_pbr = df_pbr.sort_values(by=['티커'], axis=0, na_position='last', ascending=True)
    print(df_pbr)
    df_pbr.reset_index(drop=False, inplace=True)
    print(df_pbr)
    df_pbr.set_index('티커')
    print(df_pbr)

    # #4. 기관 외인 수급
    # #기관
    # df_inst = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "기관")
    # df_inst = df_inst.replace(0, np.NaN)
    # print(df_inst)

    # df_institution = df_inst[['순매수거래대금']]
    # df_institution.insert(0, '티커', df_institution.index)
    # df_institution = df_institution.reset_index(drop=True)
    # df_institution['inst_rank'] = df_institution.index
    # print(df_institution)
    # df_institution.loc[df_institution["순매수거래대금"].isnull(), "inst_rank"] = np.NaN
    # print(df_institution)
    # df_institution = df_institution.sort_values(by=['티커'], axis=0, na_position = 'last', ascending=True)
    # df_institution.reset_index(drop=False, inplace=True)
    # df_institution.set_index('티커')
    # print(df_institution)

    # #외인
    # df_fore = stock.get_market_net_purchases_of_equities_by_ticker(date, date, "ALL", "외국인")
    # df_fore = df_fore.replace(0, np.NaN)
    # print(df_fore)

    # df_foreigner   = df_fore[['순매수거래대금']]
    # df_foreigner.insert(0, '티커', df_foreigner.index)
    # df_foreigner = df_foreigner.reset_index(drop=True)
    # df_foreigner['fore_rank'] = df_foreigner.index
    # print(df_foreigner)
    # df_foreigner.loc[df_foreigner["순매수거래대금"].isnull(), "fore_rank"] = np.NaN
    # print(df_foreigner)
    # df_foreigner = df_foreigner.sort_values(by=['티커'], axis=0, na_position = 'last', ascending=True)
    # df_foreigner.reset_index(drop=False, inplace=True)
    # df_foreigner.set_index('티커')
    # print(df_foreigner)

    df_total = df_fund[['티커']]
    df_total.set_index('티커')
    num_of_data = len(df_total)
    print(df_total)

    df_total = pd.merge(df_total, df_fund[['티커', 'fund_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_volume[['티커', 'volume_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_per[['티커', 'per_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_pbr[['티커', 'pbr_rank']], how='left', on='티커')
    print(df_total)

    # df_total = pd.merge(df_total, df_institution[['티커', 'inst_rank']], how='left',on='티커')
    # print(df_total)

    # df_total = pd.merge(df_total, df_foreigner[['티커', 'fore_rank']], how='left',on='티커')
    # print(df_total)

    df_total = pd.merge(df_total, df_5sma[['티커', 'sma5_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_20sma[['티커', 'sma20_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_60sma[['티커', 'sma60_rank']], how='left', on='티커')
    print(df_total)

    df_total = pd.merge(df_total, df_inc_rate[['티커', 'inc_rank']], how='left', on='티커')
    print(df_total)

    # 결측값이 있는 전체행 제거
    df_total = df_total.dropna(axis=0)
    print(df_total)

    # 모든 행의 합을 컬럼으로 추가
    # df_sum = df_total[['fund_rank', 'volume_rank', 'per_rank', 'pbr_rank', 'inst_rank', 'fore_rank']]
    # df_sum = df_total[['fund_rank', 'volume_rank', 'per_rank', 'pbr_rank', 'sma5_rank', 'sma20_rank', 'sma60_rank', 'inc_rank']]

    df_sum = df_total[['fund_rank', 'volume_rank', 'per_rank', 'pbr_rank', 'sma5_rank', 'sma20_rank', 'sma60_rank']]
    print(df_sum)
    df_sum["total_rank"] = df_sum.sum(axis=1)
    print(df_sum)
    df_sum.insert(0, '티커', df_total['티커'])

    print(df_sum)

    df_sum = df_sum.sort_values(by=['total_rank'], axis=0, na_position='last', ascending=True)
    df_sum = df_sum[0:int(len(df_sum))]
    print(df_sum)
    df_sum.reset_index(drop=True, inplace=True)
    print(df_sum)
    df_sum.rename({'티커': 'stock_id'}, axis='columns', inplace=True)

    print(df_sum)

    df_sum.to_csv('total_rank_' + date + '.csv')

