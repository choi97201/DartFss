import sys
from PyQt5.QtWidgets import *
from PyQt5.QAxContainer import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import datetime
import pymysql
import pandas as pd
import time
import pythoncom
import numpy as np
from tqdm import tqdm
from cjw_mysql import *
import csv
import os
from pykrx import stock

class AlgoTrader(QMainWindow):
    def __init__(self, gb):
        super().__init__()
        self.setGeometry(100, 100, 960, 800)
        self.setWindowTitle("AlgoTrader")
        self.gb = gb
        self.budget = None
        self.remained_data = False
        self.log_path = '/'.join(str(os.getcwd()).split('\\')[:-2]) + '/log.txt'
        print(self.log_path)
        self.exit_time = '15:40:00'

        self.jango_code = {}
        self.jango_code_list = []
        self.jango_name_list = []
        self.jango_amount_list = []
        self.tr_condition_loaded = False

        self.plain_text_edit = QPlainTextEdit(self)
        self.plain_text_edit.setReadOnly(True)
        self.plain_text_edit.move(10, 100)
        self.plain_text_edit.resize(582, 688)

        self.clock = QLabel("", self)
        self.clock.move(835, 10)
        self.clock.resize(150, 30)

        self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
        self.ocx.OnEventConnect.connect(self._handler_login)
        self.ocx.OnReceiveTrData.connect(self._handler_tr_data)
        self.ocx.OnReceiveTrCondition.connect(self._handler_tr_condition)
        self.ocx.OnReceiveConditionVer.connect(self._handler_condition_load)
        self.ocx.OnReceiveChejanData.connect(self._handler_chejan_data)

        self.login_event_loop = QEventLoop()
        self.CommConnect()

    def CommConnect(self):
        self.ocx.dynamicCall("CommConnect()")
        self.login_event_loop.exec()

    def CommRqData(self, rqname, trcode, next, screen_no):
        self.ocx.dynamicCall("CommRqData(QString, QString, int, QString)",
                             rqname, trcode, next, screen_no)

    def DisConnectRealData(self, screen_no):
        self.ocx.dynamicCall("DisConnectRealData(QString)", screen_no)

    def GetLoginInfo(self, tag):
        data = self.ocx.dynamicCall("GetLoginInfo(QString)", tag)
        return data

    def GetRepeatCnt(self, trcode, rqname):
        ret = self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
        return ret

    def GetConditionLoad(self, block=True):
        self.condition_loaded = False
        self.ocx.dynamicCall("GetConditionLoad()")
        if block:
            while not self.condition_loaded:
                pythoncom.PumpWaitingMessages()

    def GetConditionNameList(self):
        data = self.ocx.dynamicCall("GetConditionNameList()")
        conditions = data.split(";")[:-1]

        # [('000', 'perpbr'), ('001', 'macd'), ...]
        result = []
        for condition in conditions:
            cond_index, cond_name = condition.split('^')
            result.append((cond_index, cond_name))

        return result

    def GetCommRealData(self, code, fid):
        data = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, fid)
        return data

    def SetInputValue(self, id, value):
        self.ocx.dynamicCall("SetInputValue(QString, QString)", id, value)

    def GetCommData(self, trcode, rqname, index, item):
        data = self.ocx.dynamicCall("GetCommData(QString, QString, int, QString)",
                                    trcode, rqname, index, item)
        return data.strip()

    def GetChejanData(self, fid):
        data = self.ocx.dynamicCall("GetChejanData(int)", fid)
        return data

    def GetCodeListByMarket(self, fid):
        ret = self.ocx.dynamicCall("GetCodeListByMarket(QString)", [str(fid)])
        code_list = ret.split(';')
        return code_list

    def SendCondition(self, screen, cond_name, cond_index, search):
        self.tr_condition_loaded = False
        self.ocx.dynamicCall("SendCondition(QString, QString, int, int)", screen, cond_name, cond_index, search)
        while not self.tr_condition_loaded:
            pythoncom.PumpWaitingMessages()
        return self.tr_condition_data

    def SendOrder(self, rqname, screen, accno, order_type, code, quantity, price, hoga, order_no):
        self.ocx.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                             [rqname, screen, accno, order_type, code, quantity, price, hoga, order_no])

    def SetRealReg(self, screen_no, code_list, fid_list, real_type):
        self.ocx.dynamicCall("SetRealReg(QString, QString, QString, QString)",
                              screen_no, code_list, fid_list, real_type)

    def area2_set(self):
        self.scn_position_x = 1
        self.scn_position_y = 1
        self.scn_area_width = 0.8
        self.scn_area_height = 1

        self.code_line_edit_x = 600 * self.scn_position_x
        self.code_line_edit_y = 10 * self.scn_position_y
        self.code_line_edit_wd = 200 * self.scn_area_width
        self.code_line_edit_hg = 30 * self.scn_area_height
        self.code_line_edit = QLineEdit('종목코드', self)
        self.code_line_edit.move(self.code_line_edit_x, self.code_line_edit_y)
        self.code_line_edit.resize(self.code_line_edit_wd, self.code_line_edit_hg)

        self.amount_line_edit_x = self.code_line_edit_x
        self.amount_line_edit_y = self.code_line_edit_y + self.code_line_edit_hg + 2
        self.amount_line_edit_wd = self.code_line_edit_wd
        self.amount_line_edit_hg = self.code_line_edit_hg
        self.amount_line_edit = QLineEdit('수량', self)
        self.amount_line_edit.move(self.amount_line_edit_x, self.amount_line_edit_y)
        self.amount_line_edit.resize(self.amount_line_edit_wd, self.amount_line_edit_hg)

        self.buy_btn_x = self.code_line_edit_x
        self.buy_btn_y = self.amount_line_edit_y + self.amount_line_edit_hg + 2
        self.buy_btn_wd = self.code_line_edit_wd / 2 - 1
        self.buy_btn_hg = 50 * self.scn_area_width
        self.buy_button = QPushButton('매수', self)
        self.buy_button.move(self.buy_btn_x, self.buy_btn_y)
        self.buy_button.resize(self.buy_btn_wd, self.buy_btn_hg)
        self.buy_button.clicked.connect(self.buy_btn_clicked)

        self.sell_btn_x = self.code_line_edit_x + self.buy_btn_wd + 2
        self.sell_btn_y = self.buy_btn_y
        self.sell_btn_wd = self.buy_btn_wd
        self.sell_btn_hg = self.buy_btn_hg
        self.sell_button = QPushButton('매도', self)
        self.sell_button.move(self.sell_btn_x, self.sell_btn_y)
        self.sell_button.resize(self.sell_btn_wd, self.sell_btn_hg)
        self.sell_button.clicked.connect(self.sell_btn_clicked)

    def area3_set(self):
        self.thd_position_x = 1
        self.thd_position_y = 1
        self.thd_area_width = 1
        self.thd_area_height = 1

        self.jango_code_table_x = 600 * self.thd_position_x
        self.jango_code_table_y = 120 * self.thd_position_y
        self.jango_code_table_wd = 350 * self.thd_area_width
        self.jango_code_table_hg = 670 * self.thd_area_height
        self.jango_code_table = QTableWidget(self)
        self.jango_code_table.move(self.jango_code_table_x, self.jango_code_table_y)
        self.jango_code_table.resize(self.jango_code_table_wd, self.jango_code_table_hg)
        self.jango_code_table.setColumnCount(3)
        self.jango_code_table_get_data()

    def area4_set(self):
        self.frt_position_x = 1
        self.frt_position_y = 1
        self.frt_area_width = 1
        self.frt_area_height = 1

        self.algo1_button_x = 10 * self.frt_position_x
        self.algo1_button_y = 10 * self.frt_position_y
        self.algo1_button_wd = 289 * self.frt_area_width
        self.algo1_button_hg = 88 * self.frt_area_height
        self.algo1_button = QPushButton('전략1', self)
        self.algo1_button.move(self.algo1_button_x, self.algo1_button_y)
        self.algo1_button.resize(self.algo1_button_wd, self.algo1_button_hg)
        self.algo1_button.clicked.connect(self.algo1_btn_clicked)

        self.algo2_button_x = self.algo1_button_x + self.algo1_button_wd + 2
        self.algo2_button_y = self.algo1_button_y
        self.algo2_button_wd = self.algo1_button_wd
        self.algo2_button_hg = self.algo1_button_hg
        self.algo2_button = QPushButton('전략2', self)
        self.algo2_button.move(self.algo2_button_x, self.algo2_button_y)
        self.algo2_button.resize(self.algo2_button_wd, self.algo2_button_hg)
        self.algo2_button.clicked.connect(self.algo2_btn_clicked)

    def algo1_btn_clicked(self):
        self.run_algo1()

    def algo2_btn_clicked(self):
        self.run_algo2()

    def buy_btn_clicked(self):
        code = self.code_line_edit.text()
        amount = self.amount_line_edit.text()
        self.SendOrder("매수", "0101", self.account, 1, code, amount, 0, "03", "")
        self.code_line_edit.clear()
        self.amount_line_edit.clear()
    
    def clock_loop(self):
        curr_time = str(datetime.datetime.now()).split('.')[0]
        self.clock.setText(curr_time)

        if curr_time.split(' ')[1] > self.exit_time:

            if self.gb == 4:

                maria = Maria()
                maria.setMaria()
                maria.mariaSql(f'truncate {self.monitor_tablename}')
                for idx in range(self.have_df.shape[0]):
                    curr_code = self.have_df['code'][idx]
                    if curr_code in self.have_list:
                        self.res_df.append(list(self.have_df.iloc[idx]))
                        maria.mariaInsertData(self.monitor_tablename, tuple(list(self.have_df.iloc[idx])), 0)
                maria.mariaCommitDB()

            QCoreApplication.instance().quit()

    def slice_data(self, data):
        print('데이터를 분리합니다.')
        # exception1
        if not data:
            print('비어있는 딕셔너리 입니다.')
            return

        slicded_data = {}
        for w in data.keys():
            slicded_data[w] = data[w].iloc[-30:].reset_index(drop=True)
            curr_shape = slicded_data[w].shape[0]
            print(f'{w} rows: {curr_shape}')
        return slicded_data

    def get_data(self, data, which, wish_list):
        print('데이터를 불러옵니다.')
        print(f'which: {which}')
        print(f'wish_list: {wish_list}')
        '''
        전체 종목 일별 데이터가 있는 csv 파일들을 읽어 딕셔너리 형태로 return

        :param wish_list: 원하는 데이터의 리스트 ex) ['close', 'open']
        :param std_date: 슬라이싱 하고자 하는 날짜

        :return data: 모든 종목의 원하는 데이터를 갖고있는 딕셔너리
        '''

        for w in wish_list:
            data[f'{which}_{w}'] = pd.read_csv(
                os.path.join(self.data_path, '{}.csv'.format(w)))
            data[f'{which}_{w}'].columns = ['Date'] + [str(c).zfill(0) for c in
                                                       data[f'{which}_{w}'].columns[1:]]
        return data

    def get_hoga_unit(self, price, market):
        if market == 'kospi':
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
        elif market == 'kosdaq':
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

    def jango_code_table_get_data(self):
        self.jango_code_table.clear()
        self.request_OPW00004()
        self.jango_code_table.setRowCount(self.jango_code_list_len)
        column_headers = ['종목코드', '종목명', '수량']
        self.jango_code_table.setHorizontalHeaderLabels(column_headers)
        for i, curr_code in enumerate(list(self.jango_code.keys())):
            self.jango_code_table.setItem(i, 0, QTableWidgetItem(str(curr_code)))
            self.jango_code_table.setItem(i, 1, QTableWidgetItem(self.jango_code[curr_code][0]))
            self.jango_code_table.setItem(i, 2, QTableWidgetItem(str(int(self.jango_code[curr_code][1]))))

    def set_params(self):
        if self.gb == 1:
            self.signal_tablename = 'found_stock_list'
            self.monitor_tablename = 'algo1_ver3_monitor'
            self.monitor_df = []
            self.monitor_cols = ['날짜', '순위', '종목코드', '시가', '고가', '저가', '종가', '거래량', '전날거래량', '이평종가',
                                 '장대양봉', '이평거래량', '윗꼬리', '개인순매수', '외인순매수', '기관순매수', '상태']
            self.rank_df = None
            self.selected_codes = []
            self.buy_amounts = []
            self.buy_codes = []
            self.curr_open = None
            self.curr_close = None

            self.uptail_param = 0.22
            self.search_amount = 200
            self.stock_amount = 10
            self.close_sma_param = 10
            self.close_sma_nm = f'close{self.close_sma_param - 1}sma'
            self.volume_sma_param = 5
            self.volume_sma_nm = f'volume{self.volume_sma_param - 1}sma'
            self.foreigner_sum_param = 7
            self.foreigner_sum_nm = f'foreiger{self.foreigner_sum_param}sum'
            self.personal_sum_nm = f'personal{self.foreigner_sum_param}sum'
            self.agency_sum_nm = f'agency{self.foreigner_sum_param}sum'
            self.positive_param = 1
            self.volume_change_param = 0.9
            self.data_path = 'C:/data/algo1'

        if self.gb == 2:
            self.signal_tablename = 'found_stock_list'
            self.monitor_tablename = 'algo2_monitor'
            self.monitor_df = []
            self.exit_time = '15:18:00'
            self.etf_code = '122630'
            self.inv_code = '252670'
            self.buy_code = None
            self.quantity = None
            self.amount = None
            self.hold = None
            self.data = {}
            self.data['ETF_range'] = None
            self.data['ETF_target'] = None
            self.data['INV_range'] = None
            self.data['INV_target'] = None
            self.kaufman = None
            self.variance_range = 0.5
            self.kaufman_thr = 0.2
            self.kaufman_range = 14

        if self.gb == 4:
            self.signal_tablename = 'found_stock_list_test2'
            self.stg_name = '테스트전략12'
            self.monitor_tablename = 'algo4_have_list'
            self.stock_amount = 10
            self.exit_time = '15:30:00'

        # if self.gb == 5:
        #     self.fin_time = '13:19:50'
        #     self.signal_tablename = 'found_stock_list_test2'
        #     self.monitor_tablename = 'algo4_monitor'
        #     self.buy_list_tablename = 'algo4_buy_list'
        #     self.kosdaq_code = '229200'
        #     self.kosdaq_momentum = None
        #     self.data_path = '../'
        #     self.target_price = {}
        #     self.buy_list = []
        #     self.today_sell_list = []
        #     self.write_price_list = []
        #     self.stock_amount = 0

        if self.gb == 0:
            self.min_data = []

        if self.gb == 'theme':
            self.theme_df = []
            self.data_path = '../'

    def request_opt10086(self, code, rqname):
        now = datetime.datetime.now()
        today = now.strftime("%Y%m%d")
        self.SetInputValue("종목코드", code)
        self.SetInputValue("기준일자", today)
        self.SetInputValue("표시구분", 0)
        self.CommRqData(rqname, "opt10086", 0, '9000')
        self.request_event_loop.exec()

    def request_opw00005(self, rqname='예수금조회'):
        self.SetInputValue("계좌번호", self.account)
        self.SetInputValue("비밀번호", "")
        self.SetInputValue("비밀번호입력매체구분", "00")
        self.CommRqData(rqname, "opw00005", 0, "9003")
        self.request_event_loop.exec()

    def request_opt10030(self):
        self.SetInputValue("시장구분", '000')
        self.SetInputValue("정렬구분", "1")
        self.SetInputValue("관리종목포함", "16")
        self.SetInputValue("신용구분", '0')
        self.SetInputValue("거래량구분", '0')
        self.SetInputValue("가격구분", '0')
        self.SetInputValue("거래대금구분", '10')
        self.SetInputValue("장운영구분", '0')
        self.CommRqData("거래량상위조회", "opt10030", 0, "9004")
        self.request_event_loop.exec()
        while self.remained_data == True:
            self.SetInputValue("시장구분", '000')
            self.SetInputValue("정렬구분", "1")
            self.SetInputValue("관리종목포함", "16")
            self.SetInputValue("신용구분", '0')
            self.SetInputValue("거래량구분", '0')
            self.SetInputValue("가격구분", '0')
            self.SetInputValue("거래대금구분", '10')
            self.SetInputValue("장운영구분", '0')
            self.CommRqData("거래량상위조회", "opt10030", 2, "9004")
            self.request_event_loop.exec()
            break

    def request_opt10001(self, code, rqname):
        self.SetInputValue("종목코드", code)
        self.CommRqData(rqname, "opt10001", 0, "9005")
        self.request_event_loop.exec()

    def request_OPW00004(self):
        self.SetInputValue("계좌번호", self.account)
        self.SetInputValue("비밀번호", '')
        self.SetInputValue('상장폐지조회구분', '0')
        self.SetInputValue('비밀번호입력매체구분', '00')
        self.CommRqData("보유종목조회", "OPW00004", 0, "9004")
        self.request_event_loop.exec()

    def request_OPT90001(self):
        self.SetInputValue("검색구분", '0')
        self.SetInputValue("종목코드", '')
        self.SetInputValue('날짜구분', '1')
        self.SetInputValue('테마명', '')
        self.SetInputValue('등락수익구분', '1')
        self.CommRqData("테마조회", "OPT90001", 0, "9004")
        self.request_event_loop.exec()

    def sell_btn_clicked(self):
        code = self.code_line_edit.text()
        amount = self.amount_line_edit.text()
        self.SendOrder('매도', '0101', self.account, 2, code, amount, 0, "03", "")
        self.code_line_edit.clear()
        self.amount_line_edit.clear()

    def write_log(self, info):
        curr_time = str(datetime.datetime.now())
        info = f'[{curr_time}] ' + info
        print(info)
        self.plain_text_edit.appendPlainText(info)
        f = open(self.log_path, 'a', encoding='cp949', newline='')
        wr = csv.writer(f)
        wr.writerow([info])
        f.close()

    def _algo2_real_data(self, real_type, code):
        if real_type == "주식체결":
            if self.kaufman >= self.kaufman_thr:
                if code == '122630':
                    curr_stock = 'ETF'
                else:
                    curr_stock = 'INV'
                # 현재가
                curr_price = abs(int(self.GetCommRealData(code, 10)))
                trade_time = self.GetCommRealData(code, 20)

                # 목표가 계산
                # TR 요청을 통한 전일 range가 계산되었고 아직 당일 목표가가 계산되지 않았다면
                if self.data[f'{curr_stock}_range'] is not None and self.data[f'{curr_stock}_target'] is None:
                    curr_open = abs(int(self.GetCommRealData(code, 16)))
                    self.data[f'{curr_stock}_target'] = int(
                        curr_open + (self.data[f'{curr_stock}_range'] * self.variance_range))
                    info = f"{curr_stock} Target Price Calculated[{self.data[f'{curr_stock}_target']}]"
                    self.write_log(info)

                # 매수시도

                # 당일 매수하지 않았고
                # TR 요청과 Real을 통한 목표가가 설정되었고
                # TR 요청을 통해 잔고조회가 되었고
                # 현재가가 목표가가 이상이면
                condition = self.hold is None and self.data[f'{curr_stock}_target'] is not None and curr_price >= \
                            self.data[
                                f'{curr_stock}_target']

                if condition:
                    self.hold = True
                    # if self.real_invest:
                    #     self.quantity = int(self.budget / curr_price)
                    #     self.quantity = 1
                    #     self.SendOrder("매수", "7000", self.account, 1, code, self.quantity, 0, "03", "")
                    #     info = f"시장가 매수 진행 수량: {self.quantity}"
                    #     self.write_log(info)
                    curr_time = str(datetime.datetime.now()).split('.')[0]
                    table_data = [
                        'BUY',
                        curr_time,
                        '테스트전략11',
                        code,
                        1,
                        0,
                        'Auto',
                        0,
                        0,
                        1,
                        'Null',
                        'Null',
                        '15:18:00',
                        'time']
                    print(table_data)
                    maria = Maria()
                    maria.setMaria()
                    maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
                    maria.mariaCommitDB()
                    self.monitor_df = [curr_time, 'Trade', code, curr_price,
                                       self.data[f'{curr_stock}_target'], self.kaufman]
                    print(self.monitor_df)
                    maria.mariaInsertData(self.monitor_tablename, tuple(self.monitor_df), 0)
                    maria.mariaCommitDB()
                    info = f"{curr_stock} Target[{self.data[f'{curr_stock}_target']}] Price[{curr_price}] 매수 주문 요청"
                    self.write_log(info)

                    self.buy_code = code
                    time.sleep(3)
                    QCoreApplication.instance().quit()
                # 로깅
                if self.hold is None:
                    self.plain_text_edit.appendPlainText(
                        f"{curr_stock} Time[{trade_time}] Target[{self.data[f'{curr_stock}_target']}] Price[{curr_price}]")

    # def _algo4_real_data(self, real_type, code):
    #     if code not in self.buy_list and len(self.buy_list) < 10 and code not in self.today_sell_list:
    #         if real_type == '주식체결':
    #             curr_price = abs(int(self.GetCommRealData(code, 10)))
    #             curr_target_price = self.target_price[code][0]
    #             curr_hoga = self.target_price[code][1]
    #
    #             condition = curr_target_price - curr_hoga <= curr_price <= curr_target_price + curr_hoga
    #             if code not in self.write_price_list:
    #                 self.write_price_list.append(code)
    #                 info = f'[{len(self.write_price_list)}/{len(list(self.target_price.keys()))}] 종목코드[{code}] 현재가[{curr_price}] 타겟가격[{curr_target_price}]]'
    #                 self.write_log(info)
    #             if condition:
    #                 info = f"[{code}] 매도 신호 발생"
    #                 self.write_log(info)
    #                 self.buy_list.append(code)
    #                 curr_datetime = str(datetime.datetime.now()).split('.')[0]
    #                 table_data = [
    #                               'BUY',
    #                               curr_datetime,
    #                               '테스트전략7',
    #                               code,
    #                               1,
    #                               0,
    #                               'Auto',
    #                               0,
    #                               0,
    #                               0,
    #                             'Null',
    #                             'Null',
    #                             'Null',
    #                             'Null']
    #                 self.maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
    #                 self.maria.mariaCommitDB()
    #
    #                 monitor_data = [code, curr_datetime]
    #                 self.maria.mariaInsertData(self.monitor_tablename, tuple(monitor_data), 1)
    #                 self.maria.mariaInsertData(self.buy_list_tablename, tuple(monitor_data), 1)
    #                 self.maria.mariaCommitDB()
    #
    #
    #                 amount = int((self.budget * 0.98 / self.stock_amount) / curr_price)
    #                 if self.real_invest:
    #                     self.SendOrder("매수", "0101", self.account, 1, code, amount, 0, "03", "")
    #                     time.sleep(0.5)

    def _algo4_real_data(self, real_type, code):

        if real_type == '주식체결':
            curr_price = abs(int(self.GetCommRealData(code, 10)))

            # 매수대상
            if code in self.to_buy_list and code not in list(self.jango_code.keys()):

                trade_time = self.GetCommRealData(code, 20)
                self.plain_text_edit.appendPlainText(
                    f"{code} Time[{trade_time}] BUY Target[{self.buy_target_price[code]}] Price[{curr_price}]"
                )

                if self.buy_target_price[code] > curr_price:
                    amount = int((self.budget * 0.98 / self.stock_amount) / curr_price)

                    if self.real_invest:
                        self.SendOrder("매수", "0101", self.account, 1, code, amount, 0, "03", "")
                        time.sleep(2)
                        self.to_buy_list.remove(code)
                        self.have_list.append(code)
                        self.sell_target_price[code] = curr_price * 1.01
                        self.res_df.append([code, curr_price, str(datetime.datetime.now()).split('.')[0]])
                        self.jango_code[code] = ['_', code, amount]



            # 매도대상
            elif code in self.have_list:

                trade_time = self.GetCommRealData(code, 20)
                self.plain_text_edit.appendPlainText(
                    f"{code} Time[{trade_time}] SELL Target[{self.sell_target_price[code]}] Price[{curr_price}]"
                )
                if self.sell_target_price[code] < curr_price:

                    if self.real_invest and code in list(self.jango_code.keys()):
                        amount = self.jango_code[code][1]
                        self.SendOrder("매도", "0101", self.account, 2, code, amount, 0, "03", "")
                        time.sleep(0.5)
                        self.have_list.remove(code)


    def _handler_chejan_data(self, gubun, item_cnt, fid_list):
        if gubun == '0':
            체결시간 = str(self.GetChejanData('908'))
            체결시간 = '{}:{}:{}'.format(체결시간[:2], 체결시간[2:4], 체결시간[4:])
            주문상태 = self.GetChejanData('913')
            종목코드 = self.GetChejanData('9001')
            주문수량 = self.GetChejanData('900')
            if 주문상태 == '접수':
                info = f'[{체결시간}][{주문상태}][{종목코드}] 주문수량[{주문수량}]'
            elif 주문상태 == '체결':
                체결가 = self.GetChejanData('910')
                체결량 = self.GetChejanData('911')
                info = f'[{체결시간}][{주문상태}][{종목코드}] 주문수량[{주문수량}] 체결가[{체결가}] 체결량[{체결량}]'
            self.write_log(info)

    def _handler_login(self, err_code):
        if err_code == 0:
            self.plain_text_edit.appendPlainText("로그인 완료")
        self.login_event_loop.exit()

    def _handler_real_data(self, code, real_type, real_data):
        # if real_type == "장시작시간":
        #     gubun = self.GetCommRealData(code, 215)
        #     if gubun == '4':
        #         QCoreApplication.instance().quit()
        #         print("장 시작 시간이 아닙니다. 메인 윈도우 종료")
        curr_time = str(datetime.datetime.now()).split('.')[0]
        if self.gb == 2:
            self._algo2_real_data(real_type, code)

        elif self.gb == 4:
            self._algo4_real_data(real_type, code)

    def _handler_tr_data(self, screen_no, rqname, trcode, record, next):
        if rqname == '일별주가조회':
            cnt = self.GetRepeatCnt(trcode, rqname)
            if cnt > 10:
                self.prev_close = abs(int(self.GetCommData(trcode, rqname, 1, "종가")))
                self.curr_open = abs(int(self.GetCommData(trcode, rqname, 0, "시가")))
                self.curr_high = abs(int(self.GetCommData(trcode, rqname, 0, "고가")))
                self.curr_low = abs(int(self.GetCommData(trcode, rqname, 0, "저가")))
                self.curr_volume = abs(int(self.GetCommData(trcode, rqname, 0, "거래량")))
                self.curr_close = abs(int(self.GetCommData(trcode, rqname, 0, "종가")))
                self.pivot = (self.curr_high + self.curr_low + self.curr_close) / 3 * 1.01

                self.volume_sma = []
                for i in range(self.volume_sma_param):
                    self.volume_sma.append(abs(int(self.GetCommData(trcode, rqname, i, "거래량"))))
                self.volume_sma = sum(self.volume_sma) / self.volume_sma_param

            else:
                self.prev_close = 0

        elif rqname == '예수금조회':
            self.budget = int(self.GetCommData(trcode, rqname, 0, "예수금D+2"))
            info = f'매입가능금액: {self.budget}'
            self.write_log(info)

        elif rqname == '보유종목조회':
            self.jango_code_list_len = self.GetRepeatCnt(trcode, rqname)
            for i in range(self.jango_code_list_len):
                curr_code = self.GetCommData(trcode, rqname, i, "종목코드")
                curr_name = self.GetCommData(trcode, rqname, i, "종목명")
                curr_amount = self.GetCommData(trcode, rqname, i, "보유수량")
                self.jango_code[curr_code] = [curr_name, curr_amount, i]

        elif rqname == '거래량상위조회':
            if self.rank_df is None:
                data_list = []
                for i in range(100):
                    curr_code = self.GetCommData(trcode, rqname, i, "종목코드")
                    curr_volume = abs(int(self.GetCommData(trcode, rqname, i, "거래량")))
                    data_list.append([curr_code, curr_volume])
                self.rank_df = pd.DataFrame(data_list, columns=['종목코드', '거래량'])
            else:
                data_list = []
                for i in range(100):
                    curr_code = self.GetCommData(trcode, rqname, i, "종목코드")
                    curr_volume = abs(int(self.GetCommData(trcode, rqname, i, "거래량")))
                    data_list.append([curr_code, curr_volume])
                self.rank_df = pd.concat([self.rank_df, pd.DataFrame(data_list, columns=['종목코드', '거래량'])]).reset_index(
                    drop=True)
            if next == '2':
                self.remained_data = True
            else:
                self.remained_data = False
        elif rqname == "ETF일봉데이터":
            now = datetime.datetime.now()
            today = now.strftime("%Y%m%d")
            curr_date = self.GetCommData(trcode, rqname, 0, "날짜")

            # 장시작 후 TR 요청하는 경우 0번째 row에 당일 일봉 데이터가 존재함
            kodex_close_list = []
            if curr_date != today:
                curr_high = abs(int(self.GetCommData(trcode, rqname, 0, "고가")))
                curr_low = abs(int(self.GetCommData(trcode, rqname, 0, "저가")))
                kaufman_start_date = self.GetCommData(trcode, rqname, self.kaufman_range - 1, "날짜")
                kaufman_last_date = self.GetCommData(trcode, rqname, 0, "날짜")
                for i in range(self.kaufman_range):
                    kodex_close_list.append(abs(int(self.GetCommData(trcode, rqname, i, "종가"))))
            else:
                curr_high = abs(int(self.GetCommData(trcode, rqname, 1, "고가")))
                curr_low = abs(int(self.GetCommData(trcode, rqname, 1, "저가")))
                kaufman_start_date = self.GetCommData(trcode, rqname, self.kaufman_range, "날짜")
                kaufman_last_date = self.GetCommData(trcode, rqname, 1, "날짜")
                for i in range(1, self.kaufman_range + 1):
                    kodex_close_list.append(abs(int(self.GetCommData(trcode, rqname, i, "종가"))))

            self.data['ETF_range'] = int(curr_high) - int(curr_low)
            info = f"ETF: High[{curr_high}] Low[{curr_low}] Range[{self.data['ETF_range']}]"
            self.write_log(info)

            self.kaufman = abs(kodex_close_list[0] - kodex_close_list[-1]) / sum(
                [abs(kodex_close_list[i] - kodex_close_list[i + 1]) for i in range(len(kodex_close_list) - 1)])
            info = f"DateRange[{kaufman_start_date} - {kaufman_last_date}] Kaufman[{self.kaufman}]"
            self.write_log(info)

            curr_time = str(datetime.datetime.now()).split('.')[0]
            if self.kaufman < self.kaufman_thr:
                self.monitor_df = [curr_time, 'Kaufman', 'None', 0, 0, self.kaufman]
                print(self.monitor_df)
                maria = Maria()
                maria.setMaria()
                maria.mariaInsertData('algo2_monitor', tuple(self.monitor_df), 0)
                maria.mariaCommitDB()
        elif rqname == 'INV일봉데이터':
            now = datetime.datetime.now()
            today = now.strftime("%Y%m%d")
            curr_date = self.GetCommData(trcode, rqname, 0, "날짜")

            if curr_date != today:
                curr_high = abs(int(self.GetCommData(trcode, rqname, 0, "고가")))
                curr_low = abs(int(self.GetCommData(trcode, rqname, 0, "저가")))
            else:
                curr_date = self.GetCommData(trcode, rqname, 1, "날짜")
                curr_high = abs(int(self.GetCommData(trcode, rqname, 1, "고가")))
                curr_low = abs(int(self.GetCommData(trcode, rqname, 1, "저가")))

            self.data['INV_range'] = int(curr_high) - int(curr_low)
            info = f"INV: Date[{curr_date}] High[{curr_high}] Low[{curr_low}] Range[{self.data['INV_range']}]"
            self.plain_text_edit.appendPlainText(info)
        elif rqname == "계좌평가현황":
            rows = self.GetRepeatCnt(trcode, rqname)
            for i in range(rows):
                code = self.GetCommData(trcode, rqname, i, "종목코드")
                if code[1:] == self.buy_code:
                    self.amount = self.GetCommData(trcode, rqname, i, "보유수량")
        elif rqname == '테마조회':
            cnt = self.GetRepeatCnt(trcode, rqname)
            cols = ['테마명', '상승종목수', '하락종목수', '기간수익률', '주요종목']
            for i in range(cnt):
                curr_data = []
                for col in cols:
                    curr_data.append(self.GetCommData(trcode, rqname, i, col))
                self.theme_df.append(curr_data)
            self.theme_df = pd.DataFrame(self.theme_df, columns=cols)
        elif rqname == '주가조회':
            self.curr_price = int(self.GetCommData(trcode, rqname, 0, '종가'))
        elif rqname == '상한가조회':
            self.curr_sang = abs(int(self.GetCommData(trcode, rqname, 0, "상한가")))
        self.request_event_loop.exit()

    def _handler_tr_condition(self, screen_no, code_list, cond_name, cond_index, next):
        codes = code_list.split(';')[:-1]
        self.tr_condition_data = codes
        self.tr_condition_loaded = True

    def _handler_condition_load(self, ret, msg):
        if ret == 1:
            self.condition_loaded = True

    def run(self):
        accounts = self.GetLoginInfo("ACCNO")
        self.account = accounts.split(';')[0]
        if self.account == '8161995311':
            print('choi9720: 1호(ver.2), 2호')
            print('모의계좌입니다.')
        elif self.account == '5109955311':
            print('choi9720: 1호(ver.2), 2호')
            print('실계좌입니다.')
        elif self.account == '5847087310':
            print('choi5270: 4호')
            print('실계좌입니다.')
        elif self.account == '5872445310':
            print('choi0004: 4호(미완성)')
            print('실계좌입니다.')
        else:
            print(self.account)
        self.write_log(f'계좌번호[{self.account}]')

        # Tr 요청
        self.request_event_loop = QEventLoop()

        self.area2_set()
        self.area3_set()
        self.area4_set()

        self.clock_loop_timer = QTimer(self)
        self.clock_loop_timer.start(1000)
        self.clock_loop_timer.timeout.connect(self.clock_loop)

        # 잔고 조회
        self.request_opw00005()

    def run_algo1(self):
        info = '1호 알고리즘을 실행합니다.'
        self.write_log(info)
        self.gb = 1
        self.set_params()
        
        # 전날까지의 데이터 로딩
        self.write_log(f'csv 파일 로딩')
        data = {}
        which = 'algo1'
        wish_list = ['open', 'high', 'low', 'close', 'volume', 'volume_fillna', 'foreigner', 'personal', 'agency',
                     self.close_sma_nm, self.volume_sma_nm]
        data = self.get_data(data, which, wish_list)
        data = self.slice_data(data)

        # 조건 검색식 요청
        self.write_log('조건검색식 요청')
        self.GetConditionLoad()
        self.GetConditionNameList()
        time.sleep(0.5)

        condition_name = 'algo1'
        condition_index = '000'
        self.selected_codes = self.SendCondition('0101', condition_name, condition_index, 0)
        self.write_log(f'검색된 종목 수[{len(self.selected_codes)}]')
        time.sleep(0.5)


        for idx in tqdm(range(len(self.selected_codes))):
            curr_code = self.selected_codes[idx]
            is_continue = False
            status = 'Reset'

            # 일봉 데이터 조회 (윗꼬리, 양봉 조회)
            self.request_opt10086(curr_code, '일별주가조회')
            time.sleep(0.7)
            curr_time = str(datetime.datetime.now()).split('.')[0]
            monitor_data = [curr_time, idx, curr_code, self.curr_open, self.curr_high, self.curr_low, self.curr_close,
                                 self.curr_volume]

            close_sma, prev_volume, positive, curr_uptail, prev_foreign, prev_personal, prev_institution = 0, 0, 0, 0, 0, 0, 0

            try:
                close_sma = data[self.close_sma_nm][curr_code].iloc[-1]
                # self.volume_sma = data[self.self.volume_sma_nm][curr_code].iloc[-1]
            except:
                is_continue = True
                status = 'No Code in CSV'

            if not is_continue and self.pivot > self.curr_close:
                is_continue = True
                status = 'Pivot'

            if not is_continue and self.prev_close == 0:
                is_continue = True
                status = 'Not Enough Dates'

            if not is_continue and (close_sma == 0 or self.volume_sma == 0):
                is_continue = True
                status = 'Not Enough Dates'

            if not is_continue:
                prev_volume = data['volume_fillna'][curr_code].iloc[-1]
                if prev_volume == 0:
                    is_continue = True
                    status = 'Not Enough Dates'
                elif self.curr_volume / prev_volume < self.volume_change_param:
                    is_continue = True
                    status = 'Prev Volume'

            if not is_continue:
                positive = self.curr_close / self.curr_open
                if positive < self.positive_param:
                    is_continue = True
                    status = 'Positive Candle'

            if not is_continue:
                curr_uptail = (self.curr_high - self.curr_close) / (self.curr_high - self.curr_low)
                if curr_uptail > self.uptail_param:
                    is_continue = True
                    status = 'Up Tail'

            if not is_continue:
                close_sma = (close_sma * (self.close_sma_param - 1) + self.curr_close) / self.close_sma_param
                if self.curr_close < close_sma:
                    is_continue = True
                    status = 'Close SMA'

            if not is_continue:
                # volume_sma = (volume_sma * (self.volume_sma_param - 1) + self.curr_volume) / self.volume_sma_param
                if self.curr_volume < self.volume_sma:
                    is_continue = True
                    status = 'Volume SMA'

            if not is_continue:
                prev_personal = []
                prev_foreign = []
                prev_institution = []
                idx = 0
                date_idx = data['foreigner'].shape[0] - 1
                while True:
                    if (len(prev_personal) == self.foreigner_sum_param) or idx > date_idx:
                        break
                    prev_curr_close = data['close'][curr_code].iloc[date_idx - idx]
                    if prev_curr_close == 0:
                        idx += 1
                        pass
                    else:
                        try:
                            prev_personal.append(data['personal'][curr_code].iloc[date_idx - idx])
                            prev_foreign.append(data['foreigner'][curr_code].iloc[date_idx - idx])
                            prev_institution.append(data['agency'][curr_code].iloc[date_idx - idx])
                            idx += 1
                        except:
                            status = 'No Code in CSV'
                            is_continue = True

                if len(prev_personal) < self.foreigner_sum_param:
                    status = 'Not Enough Dates'
                    is_continue = True

                prev_personal = sum(prev_personal)
                prev_foreign = sum(prev_foreign)
                prev_institution = sum(prev_institution)

                personal_score = 1 if prev_personal < 0 else 0
                foreign_score = 1 if prev_foreign > 0 else 0
                institution_score = 1 if prev_institution > 0 else 0

                if personal_score + foreign_score + institution_score <= 0:
                    status = 'Foreigner Score'
                    is_continue = True

            monitor_data += [prev_volume, close_sma, positive, self.volume_sma, curr_uptail, prev_personal, prev_foreign, prev_institution]
            if is_continue:
                monitor_data.append(status)
                self.monitor_df.append(monitor_data)
                continue
            
            status = 'Trade'
            monitor_data.append(status)
            self.monitor_df.append(monitor_data)

            self.buy_codes.append(curr_code)
            # 실계좌 투자일 경우 매수량 설정
            if self.real_invest:
                curr_amount = int((self.budget / self.stock_amount) / self.curr_close)
                self.buy_amounts.append(curr_amount)
            if len(self.buy_codes) >= self.stock_amount:
                break

        self.write_log(f'매수 주식 수[{len(self.buy_codes)}]')
        maria = Maria()
        maria.setMaria()
        if self.send_signal:
            maria.mariaSql('truncate algo1_buy_list')
        for j, code in enumerate(self.buy_codes):
            if self.send_signal:
                curr_datetime = str(datetime.datetime.now()).split('.')[0]
                table_data = [
                              'BUY',
                              curr_datetime,
                              'AI.P',
                              code,
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
                maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
                maria.mariaCommitDB()
                maria.mariaInsertData('algo1_buy_list', tuple([str(datetime.datetime.now()).split('.')[0], code]), 1)
                maria.mariaCommitDB()
                self.write_log(f'Stock[{code}] 매수 신호 발생')

            # 실계좌 투자하는 경우
            if self.real_invest:
                curr_amount = self.buy_amounts[j]
                self.SendOrder("매수", "7000", self.account, 1, code, curr_amount, 0, "03", "")
                time.sleep(5)

        self.monitor_df = pd.DataFrame(self.monitor_df, columns=self.monitor_cols)
        for i in range(self.monitor_df.shape[0]):
            if self.send_signal:
                maria.mariaInsertData(self.monitor_tablename, tuple(list(self.monitor_df.iloc[i])), 0)
        maria.mariaCommitDB()

    def run_algo2(self):
        info = '2호 알고리즘을 실행합니다.'
        self.write_log(info)
        self.gb = 2
        self.set_params()

        self.request_opt10086(self.etf_code, 'ETF일봉데이터')
        self.request_opt10086(self.inv_code, 'INV일봉데이터')

        if self.kaufman < self.kaufman_thr:
            info = f"Kafman[{self.kaufman}] Target[{self.kaufman_thr}] Do Not Connect Real Time Data"
            self.write_log(info)

        self.ocx.OnReceiveRealData.connect(self._handler_real_data)
        self.SetRealReg('2', f'{self.etf_code};{self.inv_code}', "20", 0)
        self.SetRealReg('1', "", "215", 0)

    # def run_algo4(self):
    #     info = '4호 알고리즘을 실행합니다.'
    #     self.write_log(info)
    #     self.gb = 4
    #     self.set_params()
    #
    #     info = '조건 검색식 요청'
    #     self.write_log(info)
    #
    #     condition_name = 'algo4'
    #     condition_index = '001'
    #     tmp = self.SendCondition('0101', condition_name, condition_index, 0)
    #     self.write_log(f'검색된 종목 수[{len(self.tmp)}]')
    #     time.sleep(0.5)
    #
    #     info = 'csv 파일 로딩'
    #     self.write_log(info)
    #     stock_info = pd.read_csv('algo4_stock_info.csv')
    #     change_df = pd.read_csv('algo4_change_close2close.csv')
    #
    #     change_df = pd.DataFrame(change_df.iloc[-1]).reset_index(drop=False)
    #     change_df.columns = ['code', 'change']
    #     change_df = change_df.drop(0).reset_index(drop=True)
    #     change_df = change_df.sort_values('change', ascending=False).reset_index(drop=True).iloc[:self.rank_code_param]
    #
    #     sector_df = pd.merge(change_df, stock_info[['code', 'sector']], on=['code']).reset_index(drop=True)
    #     sector_df['change'] = sector_df['change'].astype('float')
    #     sector_change_avg = pd.DataFrame(sector_df.groupby('sector').mean()).reset_index(drop=False)
    #     sector_change_avg.columns = ['sector', 'change_avg']
    #     sector_change_avg = sector_change_avg.sort_values('change_avg', ascending=False).reset_index(drop=True)
    #
    #     rank3_sectors = sector_change_avg['sector'].tolist()[:self.rank_sector_param]
    #     rank_df = sector_df[sector_df.apply(lambda x: x['sector'] in rank3_sectors, axis=1)]
    #
    #     selected_codes = rank_df['code'].tolist()
    #     last_idx = max(len(selected_codes), self.stock_amount)
    #     selected_codes = [curr_code for curr_code in selected_codes if curr_code in tmp][:last_idx]
    #
    #     # maria = Maria()
    #     # maria.setMaria()
    #     # for code in selected_codes:
    #     #     if self.send_signal:
    #     #         curr_datetime = str(datetime.datetime.now()).split('.')[0]
    #     #         table_data = [
    #     #                       'BUY',
    #     #                       curr_datetime,
    #     #                       '테스트전략7',
    #     #                       code,
    #     #                       1,
    #     #                       0,
    #     #                       'Auto',
    #     #                       0,
    #     #                       0,
    #     #                       0,
    #     #                     'Null',
    #     #                     'Null',
    #     #                     'Null',
    #     #                     'Null']
    #     #         maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
    #     #         maria.mariaCommitDB()
    #     #         self.write_log(f'Stock[{code}] 매수 신호 발생')
    #     #
    #     #     # 실계좌 투자하는 경우
    #     #     if self.real_invest:
    #     #         curr_amount = self.buy_amounts[j]
    #     #         self.SendOrder("매수", "7000", self.account, 1, code, curr_amount, 0, "03", "")
    #     #         time.sleep(1)

    def run_algo4(self):
        info = '4호 알고리즘을 실행합니다.'
        self.write_log(info)
        self.gb = 4
        self.set_params()
        self.res_df = []
        
        info = '보유목록을 가져옵니다'
        self.write_log(info)
        self.maria = Maria()
        self.maria.setMaria()
        self.have_df = self.maria.mariaShowData(self.monitor_tablename)
        self.have_list = self.have_df['code'].tolist()
        self.sell_target_price = {}

        for idx in range(self.have_df.shape[0]):
            curr_code = self.have_df['code'][idx]
            buy_price = self.have_df['buy_price'][idx]
            self.sell_target_price[curr_code] = buy_price * 1.01

        info = '보유 종목의 per을 확인합니다'
        self.write_log(info)
        prev_close_df = pd.read_csv('../algo1_close.csv')
        prev_date = str(prev_close_df['Date'].iloc[-1]).replace('-', '')
        per_df = stock.get_market_fundamental_by_ticker(prev_date, 'ALL').reset_index(drop=False)

        to_sell_list = []

        for code in self.have_list:

            if per_df[per_df['티커'] == code]['PER'].iloc[0] > 11 or per_df[per_df['티커'] == code]['PER'].iloc[0] < 1:
                to_sell_list.append(code)

        info = f'PER 부적합 종목 수[{len(to_sell_list)}]'
        self.write_log(info)

        for curr_code in to_sell_list:
            curr_datetime = str(datetime.datetime.now()).split('.')[0]
            table_data = [
                'SELL',
                curr_datetime,
                self.stg_name,
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
            self.maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
            self.maria.mariaCommitDB()
            self.today_sell_list.append(curr_code)
            self.stock_amount -= 1
            if self.real_invest and curr_code in list(self.jango_code.keys()):
                curr_amount = self.jango_code[curr_code][1]
                self.SendOrder('매도', '0101', self.account, 2, curr_code, curr_amount, 0, "03", "")
                time.sleep(0.5)

        cap_df = stock.get_market_cap_by_ticker(prev_date, 'ALL').reset_index(drop=False)
        cap_df = cap_df.sort_values('시가총액', ascending=False).reset_index(drop=True)[:int(cap_df.shape[0] * 0.1)]
        df = pd.merge(cap_df, per_df, on=['티커'])
        df = df[(df['PER'] > 2) & (df['PER'] < 10)].reset_index(drop=True)


        self.buy_target_price = {}

        for idx in range(df.shape[0]):
            curr_code = df['티커'][idx]
            prev_close = df['종가'][idx]
            self.buy_target_price[curr_code] = prev_close * 0.98

        self.to_buy_list = df['티커'].tolist()
        self.to_buy_list = [code for code in self.to_buy_list if code not in self.have_list][:self.stock_amount - len(self.have_list)]

        code_list_to_str = ';'.join(self.to_buy_list + self.have_list)

        self.ocx.OnReceiveRealData.connect(self._handler_real_data)
        self.SetRealReg('2', code_list_to_str, "20", 0)
        self.SetRealReg('1', "", "215", 0)

    def run_algo5(self):
        info = '5호 알고리즘을 실행합니다.'
        self.write_log(info)
        self.gb = 5
        self.set_params()
        self.maria = Maria()
        self.maria.setMaria()
        self.maria.mariaSql(f'truncate {self.monitor_tablename}')

        close_df = pd.read_csv(os.path.join(self.data_path, 'algo5_close.csv'))
        sep_df = pd.read_csv(os.path.join(self.data_path, 'algo5_close5sep.csv'))
        rank_df = pd.read_csv(os.path.join(self.data_path, 'algo5_rank.csv'))

        info = '선별된 종목 조회'
        self.write_log(info)
        selected_codes_list = rank_df['code'].tolist()

        info = '코스닥(모멘텀) 및 지지선 조회'
        self.write_log(info)
        kosdaq_close_list = close_df['229200'].tolist()
        kosdaq_close_list = [curr_close for curr_close in kosdaq_close_list if curr_close != 0]
        kosdaq_close20ma = sum(kosdaq_close_list[-20:]) / 20
        kosdaq_prev_close = kosdaq_close_list[-1]
        self.kosdaq_momentum = True if kosdaq_close20ma < kosdaq_prev_close else False
        kosdaq_minimum_close = min(kosdaq_close_list[-20:-1])

        have_df = self.maria.mariaShowData(self.buy_list_tablename)
        have_list = have_df['code'].tolist()
        self.stock_amount = len(have_list)

        if kosdaq_minimum_close > kosdaq_prev_close:
            info = '코스닥 지지선 하방돌파. 보유종목 일괄 매도'
            self.write_log(info)
        else:
            info = '매도가능종목 매도'
            self.write_log(info)
        sep_thr = 100 if self.kosdaq_momentum else 90

        for curr_code in have_list:
            prev_sep = sep_df[curr_code].iloc[-1]
            if kosdaq_minimum_close > kosdaq_prev_close or prev_sep >= sep_thr:
                info = f'[{curr_code}] 매도신호 발생'
                self.write_log(info)
                curr_datetime = str(datetime.datetime.now()).split('.')[0]
                table_data = [
                    'SELL',
                    curr_datetime,
                    '테스트전략7',
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
                self.maria.mariaInsertData(self.signal_tablename, tuple(table_data), 1)
                self.maria.mariaCommitDB()
                self.today_sell_list.append(curr_code)
                self.stock_amount -= 1
                if self.real_invest and curr_code in list(self.jango_code.keys()):
                    curr_amount = self.jango_code[curr_code][1]
                    self.SendOrder('매도', '0101', self.account, 2, curr_code, curr_amount, 0, "03", "")
                    time.sleep(0.5)

        time.sleep(10)
        # 실계좌 투자하는 경우
        if self.real_invest:
            # 잔고 조회
            self.request_opw00005()

        new_have_df = have_df[have_df['code'].apply(lambda x: x not in self.today_sell_list)].reset_index(drop=True)
        self.maria.mariaSql(f'truncate {self.buy_list_tablename}')
        for i in range(new_have_df.shape[0]):
            self.maria.mariaInsertData(self.buy_list_tablename, tuple([new_have_df['code'].iloc[i], new_have_df['datetime'].iloc[i]]), 1)

        ################################################ 매도 끝 ########################################################

        info = '상승장입니다.' if self.kosdaq_momentum else '하락장입니다.'
        self.write_log(info)

        info = '타겟 가격 설정'
        self.write_log(info)
        close_df = close_df[selected_codes_list]
        target_rate = 0.97 if self.kosdaq_momentum else 0.95
        kospi_code_list = self.GetCodeListByMarket(0)
        for curr_code in selected_codes_list:
            curr_market = 'kospi' if curr_code in kospi_code_list else 'kosdaq'
            curr_target = close_df[curr_code].iloc[-1] * target_rate
            curr_hoga = self.get_hoga_unit(curr_target, curr_market)
            scale = curr_target // curr_hoga if curr_target % curr_hoga == 0 else (curr_target // curr_hoga) + 1
            self.target_price[curr_code] = [curr_hoga * scale, curr_hoga]

        joined_code_list = ';'.join(selected_codes_list)

        self.ocx.OnReceiveRealData.connect(self._handler_real_data)
        self.SetRealReg('2', joined_code_list, "20", 0)

    def run_theme(self):
        info = '테마 알고리즘을 실행합니다.'
        self.write_log(info)
        self.gb = 'theme'
        self.set_params()

        info = 'csv 파일 로드'
        self.write_log(info)
        data = {}
        which = 'algo_theme'

        self.write_log(f'csv 파일 로딩')
        data = {}
        which = 'algo1'
        wish_list = ['open', 'high', 'low', 'close', 'volume', 'volume_fillna', 'foreigner', 'personal', 'agency',
                     self.close_sma_nm, self.volume_sma_nm]
        data = self.get_data(data, which, wish_list)
        self.close_sma_nm = self.close_sma_n
        self.volume_sma_nm = self.volume_sma_nm

