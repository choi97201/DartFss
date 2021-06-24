from pykiwoom.kiwoom import *
import time
import pandas as pd
import datetime
import numpy as np
from timeloop import Timeloop
import pymysql

kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)
account_list = kiwoom.GetLoginInfo("ACCNO")
account = account_list[0]

class Maria():
    def setMaria(self, host='15.165.29.213', user='lt_user', password='De4IjOY32e7o', db='leaderstrading',
                 charset='utf8'):
        self.connect = pymysql.connect(host=host, user=user, password=password, db=db, charset=charset)
        self.cur = self.connect.cursor()
        return

    def mariaInsertData(self, tablename, data, start):
        df = self.mariaShowData(tablename)
        cols = list(df.columns)
        sql = "replace into {}{} ".format(tablename, tuple(cols[start:])).replace('\'', '')
        sql += ' values {};'.format(data)
        self.cur.execute(sql)

    def mariaCommitDB(self):
        self.connect.commit()
        return

    def mariaCreateTable(self, tablename, columns, columns_type):
        sql = "CREATE TABLE {} ({} {} PRIMARY KEY".format(tablename, columns[0], columns_type[0])
        try:
            for i in range(1, len(columns)):
                sql += ", {} {}".format(columns[i], columns_type[i])
            sql += ');'
            self.cur.execute(sql)
        except Exception as e:
            print(sql)
            print(e)
            pass
        return

    def mariaShowData(self, tablename, sql=None):
        try:
            if sql is None:
                self.cur.execute('select * from ' + tablename)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
                return df
            else:
                self.cur.execute(sql)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
                return df
        except Exception as e:
            print(e)
            return None


def getTradingHistory(account):
    input_ref_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')
    df = kiwoom.block_request("OPW00007",
                              주문일자=input_ref_date,
                              계좌번호=account,
                              비밀번호="",
                              비밀번호입력매체구분="00",
                              조회구분="4",
                              주식채권구분="1",
                              매도수구분="0",
                              종목코드="",
                              시작주문번호="",
                              output="계좌별주문체결내역상세",
                              next=0)[['주문번호', '주문구분', '주문시간', '종목번호', '주문수량', '체결수량', '체결단가', '주문잔량']]

    time.sleep(0.5)

    df = kiwoom.block_request("OPW00007",
                              주문일자=input_ref_date,
                              계좌번호=account,
                              비밀번호="",
                              비밀번호입력매체구분="00",
                              조회구분="4",
                              주식채권구분="1",
                              매도수구분="0",
                              종목코드="",
                              시작주문번호="",
                              output="계좌별주문체결내역상세",
                              next=0)[['주문번호', '주문구분', '주문시간', '종목번호', '주문수량', '체결수량', '체결단가', '주문잔량']]

    df['주문수량'] = df['주문수량'].astype('int')
    df['체결수량'] = df['체결수량'].astype('int')
    df['체결단가'] = df['체결단가'].astype('int')
    df['주문잔량'] = df['주문잔량'].astype('int')

    df['주문날짜'] = [input_ref_date] * df.shape[0]
    df = df[['주문번호', '주문날짜'] + list(df.columns[1:-1])]
    df = df[df['주문시간'] < '09:00:00'].reset_index(drop=True)
    return df

def sellStock(stock, num, account):
    try:
        kiwoom.SendOrder('시장가매도', '0101', account, 2, stock, int(num), 0, "03", "")
    except:
        pass

def getHaveStocks(account):
    df = kiwoom.block_request("OPW00004",
                              계좌번호=account,
                              비밀번호="",
                              상장폐지조회구분="0",
                              비밀번호입력매체구분="00",
                              output="종목별계좌평가현황",
                              next=0)
    return df

def sellAllStocks(account):
    sell_time = '08:55:00'
    signal_tablename = 'found_stock_list'
    real_invest = True

    while True:
        if str(datetime.datetime.now()).split(' ')[1].split('.')[0] > sell_time:
            break
    print(str(datetime.datetime.now()).split(' ')[1].split('.')[0])

    maria = Maria()
    maria.setMaria()
    buy_list_df = maria.mariaShowData('algo1_buy_list')

    for i in range(buy_list_df.shape[0]):
        curr_code = buy_list_df['code'].iloc[i]
        table_data = [
                      'SELL',
                      str(datetime.datetime.now()).split('.')[0],
                      'AI.P',
                      curr_code,
                      100,
                      0,
                      'Auto',
                      0,
                      0,
                      0,
                    'Null',
                    'Null',
                    'Null',
                    'Null']
        print(table_data)
        maria.mariaInsertData(signal_tablename, tuple(table_data), 1)
        maria.mariaCommitDB()

    if real_invest:
        df = getHaveStocks(account)
        for i in range(df.shape[0]):
            curr_code = df['종목코드'].iloc[i].replace('A', '')
            curr_name = df['종목명'].iloc[i]
            curr_amount = int(df['보유수량'].iloc[i])
            print('Order: Code[{}] Name[{}] Price[Market] Amount[{}]'.format(curr_code, curr_name, curr_amount))
            time.sleep(0.5)
            try:
                sellStock(curr_code, curr_amount, account)
            except:
                print('Sell: 주문실패 Code[{}] Name[{}] Price[Market] Amount[{}]'.format(curr_code, curr_name, curr_amount))

sellAllStocks(account)
