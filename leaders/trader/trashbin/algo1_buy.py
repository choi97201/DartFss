from pykiwoom.kiwoom import *
import time
import datetime
import numpy as np
from cjw_mysql import *

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)
account_list = kiwoom.GetLoginInfo("ACCNO")
account = account_list[0]




def buyStock(stock, num, account):
    try:
        kiwoom.SendOrder('시장가매수', '0101', account, 1, stock, int(num), 0, "03", "")
    except:
        pass

def buyStocks(account):
    search_amount = 200
    stock_amount = 10
    signal_tablename = 'found_stock_list'
    real_invest = True
    save_monitor = True

    uptail_param = 22
    close_sma_param = 10
    volume_sma_param = 5

    input_market = "001"
    input_sorttype = "1"
    input_except_type = "16"
    input_stock_state = "0"
    input_trading_amount_type = "0"
    input_price_type = "0"
    input_trading_fund_type = "10"
    input_operationtime_type = "0"
    kospi = kiwoom.block_request("opt10030",
                                 시장구분=input_market,
                                 정렬구분=input_sorttype,
                                 관리종목포함=input_except_type,
                                 신용구분=input_stock_state,
                                 거래량구분=input_trading_amount_type,
                                 가격구분=input_price_type,
                                 거래대금구분=input_trading_fund_type,
                                 장운영구분=input_operationtime_type,
                                 output="당일거래량상위요청",
                                 next=0)
    kospi2 = None
    while kiwoom.tr_remained:
        kospi2 = kiwoom.block_request("opt10030",
                                      시장구분=input_market,
                                      정렬구분=input_sorttype,
                                      관리종목포함=input_except_type,
                                      신용구분=input_stock_state,
                                      거래량구분=input_trading_amount_type,
                                      가격구분=input_price_type,
                                      거래대금구분=input_trading_fund_type,
                                      장운영구분=input_operationtime_type,
                                      output="당일거래량상위요청",
                                      next=2)
        break

    kospi = pd.concat([kospi, kospi2]).reset_index(drop=True)
    kospi['시장구분'] = ['kospi'] * kospi.shape[0]
    kospi['거래량'] = kospi['거래량'].astype('int')

    time.sleep(0.5)
    # 코스닥
    input_market = "101"
    kosdaq = kiwoom.block_request("opt10030",
                                  시장구분=input_market,
                                  정렬구분=input_sorttype,
                                  관리종목포함=input_except_type,
                                  신용구분=input_stock_state,
                                  거래량구분=input_trading_amount_type,
                                  가격구분=input_price_type,
                                  거래대금구분=input_trading_fund_type,
                                  장운영구분=input_operationtime_type,
                                  output="당일거래량상위요청",
                                  next=0)
    kosdaq2 = None
    while kiwoom.tr_remained:
        kosdaq2 = kiwoom.block_request("opt10030",
                                       시장구분=input_market,
                                       정렬구분=input_sorttype,
                                       관리종목포함=input_except_type,
                                       신용구분=input_stock_state,
                                       거래량구분=input_trading_amount_type,
                                       가격구분=input_price_type,
                                       거래대금구분=input_trading_fund_type,
                                       장운영구분=input_operationtime_type,
                                       output="당일거래량상위요청",
                                       next=2)
        break

    kosdaq = pd.concat([kosdaq, kosdaq2]).reset_index(drop=True)

    kosdaq['시장구분'] = ['kosdaq'] * kosdaq.shape[0]
    kosdaq['거래량'] = kosdaq['거래량'].astype('int')

    df = pd.concat([kospi, kosdaq])
    df = df.sort_values(['거래량'], ascending=False).reset_index(drop=True)

    stock_count = 0
    Buy_list = []
    monitor_df = []

    for i in range(df.shape[0]):
        if i >= search_amount:
            break

        if len(Buy_list) >= stock_amount:
            break

        stock_code = df['종목코드'].iloc[i]
        stock_name = df['종목명'].iloc[i]
        stock_volume = df['거래량'].iloc[i]
        stock_gb = df['시장구분'].iloc[i]

        input_ref_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')
        curr_price_df = getDayInfo(stock_code, input_ref_date)
        if curr_price_df.shape[0] < close_sma_param:
            continue
        time.sleep(1)

        curr_open = int(curr_price_df['시가'].iloc[0].replace('-', ''))
        curr_close = int(curr_price_df['종가'].iloc[0].replace('-', ''))
        curr_volume = int(curr_price_df['거래량'].iloc[0].replace('-', ''))

        # 음봉 제외
        if curr_close < curr_open:
            monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'PositiveCandle', stock_code, curr_open,
                               0, 0, curr_close, stock_volume, 0, 0, 0, 0, 0, stock_gb])
            continue

        # 윗꼬리 제외
        curr_high = int(curr_price_df['고가'].iloc[0].replace('-', ''))
        curr_low = int(curr_price_df['저가'].iloc[0].replace('-', ''))
        if curr_high == curr_low:
            continue

        top_tail_perc = ((curr_high - curr_close) / (curr_high - curr_low)) * 100
        if top_tail_perc > uptail_param:
            monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'UpTail', stock_code, curr_open,
                               curr_high, curr_low, curr_close, stock_volume, 0, 0, 0, 0, 0, stock_gb])
            continue

        # 상한가 제외
        prev_close = int(curr_price_df['종가'].iloc[1].replace('-', ''))
        adj_sang, _ = getLimitPrice(prev_close, stock_gb)
        if adj_sang <= curr_close:
            monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'UpperLimit', stock_code, curr_open,
                               curr_high, curr_low, curr_close, stock_volume, prev_close, 0, 0, adj_sang, 0, stock_gb])
            continue

        # 이평선이 주가보다 높은 경우
        curr_close_sma = np.average(
            np.array([int(close.replace('-', '')) for close in curr_price_df['종가'].iloc[:close_sma_param].tolist()]))
        if curr_close < curr_close_sma:
            monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'CloseSma', stock_code, curr_open,
                               curr_high, curr_low, curr_close, stock_volume, prev_close, curr_close_sma, 0, adj_sang, 0, stock_gb])
            continue

        # 5일 평균거래량 보다 금일 거래량이 큰 경우
        curr_volume_sma = np.average(
            np.array([int(volume.replace('-', '')) for volume in curr_price_df['거래량'].iloc[:volume_sma_param].tolist()]))
        if curr_volume < curr_volume_sma:
            monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'VolumeSma', stock_code, curr_open,
                               curr_high, curr_low, curr_close, stock_volume, prev_close, curr_close_sma, curr_volume_sma, adj_sang, 0, stock_gb])
            continue

        monitor_df.append([input_ref_date + '_' + str(i).zfill(3), 'Trade', stock_code, curr_open,
                           curr_high, curr_low, curr_close, stock_volume, prev_close, curr_close_sma, curr_volume_sma, adj_sang, 0, stock_gb])
        stock_count += 1
        print(
            'Rank[{}] Count[{}] Code[{}] Name[{}] Price[{}] Amount[{}] SangHanGa[{}] Close10Sma[{}] Volume5Sma[{}] ForeignSum[{}]'.format(
                str(i).zfill(3),
                str(stock_count).zfill(2),
                stock_code,
                stock_name,
                curr_close,
                stock_volume,
                adj_sang,
                curr_close_sma,
                curr_volume_sma,
                0))

        Buy_list.append([stock_code, stock_name, curr_close])

    curr_cnt = 10

    # 그 사이 종가가 변했을 것을 감안하여 예수금의 2% 삭감
    maria = Maria()
    maria.setMaria()
    maria.mariaSql('truncate algo1_buy_list')
    jango = getJangoData(account)
    print('\nDeposit[{}]'.format(jango))
    if jango < 0:
        print('예수금이 없습니다.')
    for b in Buy_list:
        curr_code = b[0]
        curr_name = b[1]
        curr_close = b[2]
        wish_amount = int((jango / curr_cnt) // curr_close)
        purchase = wish_amount * curr_close
        table_data = [
                      'BUY',
                      str(datetime.datetime.now()).split('.')[0],
                      '테스트전략4',
                      curr_code,
                      1,
                      0,
                      'Auto',
                      0,
                      0,
                      0,
                    'Null',
                    'Null',
                    'Null',
                    'Null']
        maria.mariaInsertData(signal_tablename, tuple(table_data), 1)
        maria.mariaCommitDB()
        maria.mariaInsertData('algo1_buy_list', tuple([str(datetime.datetime.now()).split('.')[0], curr_code]), 1)
        maria.mariaCommitDB()
        if wish_amount < 1:
            print('NoMoney: Code[{}] Name[{}] Close[{}] Price[Market] Amount[{}] Purchase[{}]'.format(curr_code,
                                                                                                      curr_name,
                                                                                                      curr_close,
                                                                                                      wish_amount,
                                                                                                      purchase))
        print('BuyOrder: Code[{}] Name[{}] Close[{}] Price[Market] Amount[{}] Purchase[{}]'.format(curr_code, curr_name,
                                                                                                   curr_close,
                                                                                                   wish_amount,
                                                                                                   purchase))
        try:
            if real_invest:
                buyStock(curr_code, wish_amount, account)
            print('Buy: 주문성공 Code[{}] Name[{}] Price[Market] Amount[{}]'.format(curr_code, curr_name, wish_amount))
            curr_cnt -= 1
            jango -= purchase
        except Exception as e:
            print(e)
            print('Buy: 주문실패 Code[{}] Name[{}] Price[Market] Amount[{}]'.format(curr_code, curr_name, wish_amount))
            pass
        time.sleep(0.5)
    if save_monitor:
        monitor_df = pd.DataFrame(monitor_df, columns=['DateRank', 'Status', 'Code', 'Open', 'High', 'Low', 'CLose', 'Volume', 'PrevClose', 'CloseSma',
                                                       'VolumeSma', 'Sang', 'ForeignSum', 'Market'])
        for idx in range(monitor_df.shape[0]):
            maria.mariaInsertData('algo1_monitor', tuple(monitor_df.iloc[idx]), 0)
        maria.mariaCommitDB()

def getJangoData(account):
    df = kiwoom.block_request("opw00005",
                              계좌번호=account,
                              비밀번호="",
                              비밀번호입력매체구분="00",
                              output="체결잔고",
                              next=0)
    return int(df['예수금D+2'].iloc[0]) * 0.98

def getHogaUnit(price, gb):
    if gb == 'kospi':
        if price < 1000:
            return 1
        elif price < 5000:
            return 5
        elif price < 10000:
            return 10
        elif price < 50000:
            return 50
        elif price < 100000:
            return 100
        elif price < 500000:
            return 500
        elif price >= 500000:
            return 1000
    elif gb == 'kosdaq':
        if price < 1000:
            return 1
        elif price < 5000:
            return 5
        elif price < 10000:
            return 10
        elif price < 50000:
            return 50
        else:
            return 100

def getDayInfo(stock_code, input_ref_date):
    return kiwoom.block_request("opt10086",
                                종목코드=stock_code,
                                조회일자=input_ref_date,
                                output="일별주가요청",
                                next=0)

def getForeign(stock_code):
    df = kiwoom.block_request("opt10070",
                              종목코드=stock_code,
                              output="당일주요거래원",
                              next=0)
    return int(df['외국계매수추정합'].iloc[0]) - int(df['외국계매도추정합'].iloc[0].replace('-', ''))

def getLimitPrice(base_price, market):
    # base_price : 기준가, date : 날짜 ,market : 시장구분
    # Output : 당일에 해당하는 상한가와 하한가를 반환

    limit_rate = 0.3 # 가격제한폭을 계산합니다

    # 1차 계산
    increment = base_price * limit_rate

    # 2차 계산
    scale = getHogaUnit(increment, market)
    adj_increment = int(increment / scale) * scale  # 1차 절사

    # 3차 계산
    sang = base_price + adj_increment
    ha = base_price - adj_increment
    # 상한가에 대한 가격 조정
    scale = getHogaUnit(sang, market)
    adj_sang = int(sang / scale) * scale  # 2차 절사
    # 하한가에 대한 가격 조정
    scale = getHogaUnit(ha, market)
    adj_ha = int(ha / scale) * scale  # 2차 절사

    return adj_sang, adj_ha  # 상한가와 하한가를 반환

buyStocks(account)
