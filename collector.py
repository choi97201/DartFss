import os
import csv
import math
from urllib.request import urlopen, Request

import OpenDartReader
import numpy as np
from selenium import webdriver
from pykrx import stock
import pandas as pd
import time
from bs4 import BeautifulSoup
import cjw_maria
import FinanceDataReader as fdr

import numpy as np

class Collector:
    def __init__(self, collector_type):
        # maria 연결 되어있는지 확인
        self.is_maria = False
        # dart api 연결 되어있는지 확인
        self.is_dart = False
        # chrome driver 연결 되어있는지 확인
        self.is_chrome = False

        # signet: 단기차입금, 장기차입금
        # dart: 자본금, 무형자산
        # fnguide: 나머지
        # pykex: 상장주식수
        self.collector_type = collector_type
        
        # 기준 종목코드 2016년 상장되어있던 기업
        stock_amount_2016 = stock.get_market_cap_by_ticker("20161229 ")[['상장주식수']]
        self.code_list = list(stock_amount_2016.index)

    def play(self):
        if self.collector_type == 'dart':
            self.dartCollect()
        elif self.collector_type == 'signet':
            self.signetCollect()
        elif self.collector_type == 'fnguide':
            self.fnguideCollect()
        else:
            print('잘못된 collector_type')

    def _setMaria(self):
        try:
            # maria db 연동
            host = "localhost"
            user = "root"
            password = "sa1234"
            db = "jamoo"
            self.maria = cjw_maria.MariaDB(host, user, password, db)
            self.is_maria = True
        except Exception as e:
            # 연동실패
            print('maria db 연동에 실패하였습니다.')
            print('Error msg: {}'.format(e.args))
            return

    def _setDart(self, api_key='abeae84cda5efbd4511011450af0c4aa6603d140'):
        self.dart = OpenDartReader(api_key)
        if self.dart.finstate_all(corp='005930', bsns_year=2019, reprt_code='11011') is None:
            print('api key가 만료되었습니다.')
            return
        else:
            self.is_dart = True

    def _setChrome(self):
        self.webdriver_options = webdriver.ChromeOptions()
        self.webdriver_options.add_argument('headless')
        self.chromedriver = 'chromedriver.exe'
        self.driver = webdriver.Chrome(self.chromedriver, options=self.webdriver_options)
        # self.driver = webdriver.Chrome(self.chromedriver)
        self.is_chrome = True

    def dartCollect(self):
        self._setDart()
        time.sleep(2)

        jb_tablename = 'jb_cp'
        mh_tablename = 'mh_cp'

        # 자본금 done_list
        jb_done_list = self.getDoneList(jb_tablename)
        # 무형자산 done_list
        mh_done_list = self.getDoneList(mh_tablename)

        mh_params = [['account_nm', '무형자산'],
                    ['account_id', 'ifrs-full_IntangibleAssetsOtherThanGoodwill']
                    ]
        jb_params1 = [['account_nm', '자본금'],
                    ['account_id', 'ifrs-full_IssuedCapital']
                    ]
        jb_params2 = [['account_nm', '자본금']
                    ]


        for c in self.code_list:
        # 자본 리스트에 있을 때
            if c in jb_done_list:
                # 자본리스트에 있고 무형 리스트에도 있을 때
                if c in mh_done_list:
                    # 건너뛰기 
                    continue
                # 자본리스트에 있지만 무형리스트에 없을 때
                else:
                    fs = self.getDartFs(c, False)
                    # fs_all 데이터가 존재
                    if (fs is None) == False:
                        self.loopDart(c, fs, mh_params, mh_tablename)
                    # fs_all 데이터가 존재 하지 않음 -> 무형자산 데이터 는 all에만 있으므로 continue
                    else:
                        self.writeError('error_dart', c, 'No all data')
                        continue
            # 자본 리스트에 없을 때                
            else:
                fs = self.getDartFs(c, False)
                # 자본리스트에 없고 무형리스트에도 없을 때
                if not c in mh_done_list:
                    if (fs is None) == False:
                        self.loopDart(fs, mh_params, mh_tablename)
                    else:
                        self.writeError('error_dart', c, 'No all data')
                # 자본리스트
                if (fs is None) == False:
                    self.loopDart(c, fs, jb_params1, jb_tablename)
                # fs_all 데이터가 없을 때 -> fs 데이터
                else:
                    fs = self.getDartFs(c, True)
                    # fs 데이터가 있을 때
                    if (fs is None) == False:
                        self.loopDart(c, fs, jb_params2, jb_tablename)
                    # fs 데이터도 없을 때 -> 에러 처리
                    else:
                        self.writeError('error_dart', c, 'No data in dart')

    def getDartFs(self, code, is_fs, bsns_year=2019, reprt_code='11011'):
        try:
            if is_fs:
                fs = self.dart.finstate(corp=code, bsns_year=bsns_year, reprt_code=reprt_code)
            else:
                fs = self.dart.finstate_all(corp=code, bsns_year=bsns_year, reprt_code=reprt_code)
        except:
            return None
        return fs

    def getDartList(self, fs, data_type, nmid, want_list=['bfefrmtrm_amount', 'frmtrm_amount', 'thstrm_amount']):
        try:
            fs_list = list(map(int, list(fs[fs[data_type] == nmid][want_list].iloc[0])))
            fs_list = list(np.array(fs_list) / 100000000)
        except:
           return None
        return fs_list

    def loopDart(self, code, fs, selected_params, tablename, want_list=['bfefrmtrm_amount', 'frmtrm_amount', 'thstrm_amount']):
        data_list = None
        idx = 0
        while True:
            if (data_list is None) == False:
                break
            if idx>=len(selected_params):
                break
            data_list = self.getDartList(fs, selected_params[idx][0], selected_params[idx][1], want_list=want_list)
            idx += 1
        if (data_list is None) == False:
            nameid = selected_params[idx-1][1].replace('-', '_')
            nameid = nameid.replace(' ','_')
            data_list = [code] + data_list + [nameid]
            self.maria.insertData(tablename, tuple(data_list))
            self.maria.commitDB()
            return True
        else:
            return None

    def checkFS(self, code, df):
        res = df[df['종목코드'] == code]['FS'].iloc[0]
        return res
        
    def signetCollect(self):
        if self.is_chrome == False:
            self._setChrome()
        sig_tablename = 'new_signal'
        # signet 데이터 done_list
        sig_list = self.getDoneList(sig_tablename+'2017')
        std_df = self.maria.showData('select * from fnguide2017')
        code_list = self.getDoneList('fnguide2017')
        for code in code_list:
            if code in sig_list:
                continue
            try:
                print(code)
                url = 'https://signalm.sedaily.com/StockFS/{}/goingconcern'.format(code)
                self.driver.get(url)
                time.sleep(3)
                html = self.driver.page_source
                check_fs = self.checkFS(code, std_df)
                if check_fs == '개별':
                    o_click = self.driver.find_element_by_css_selector('#StockInfoDetailTab_2 > div.sel_area > ul.link_sel > li:nth-child(3)')
                    o_click.click()
                else:
                    c_click = self.driver.find_element_by_css_selector('#StockInfoDetailTab_2 > div.sel_area > ul.link_sel > li:nth-child(2)')
                    c_click.click()
                soup = BeautifulSoup(html, 'html.parser')
                div = soup.select_one("#StockInfoDetailTab_2 > div.sec.sec2 > div > div > div.div_scroll > table")
                table = pd.read_html(str(div))[0]
            except Exception as e:
                self.writeError('error_signet', code, 'No page')
                pass
            else: 
                tmp = []
                for c in table.columns:
                    if '2017' in c or '{}'.format(years[1]) in c or '2019' in c or '2020' in c:
                        tmp.append(c)
                table = table[['항목'] + tmp]

                table.columns = [c.replace('.12(연결)', '') for c in table.columns]
                table.columns = [c.replace('.12(별도)', '') for c in table.columns]

                t1 = table[table['항목'] == '단기차입금']
                t2 = table[table['항목'] == '장기차입금']

                t = pd.concat([t1, t2])
                t = t.transpose().reset_index(drop=False)
                t.columns = t.iloc[0]
                t = t.rename(columns={'항목': '년도'})
                t = t.drop(0).reset_index(drop=True)

                years = ['2017', '2018', '2019']
                
                for y in years:
                    try:
                        tmp = list(t[t['년도'] == y][['단기차입금', '장기차입금']].reset_index(drop=True).iloc[0])
                        self.maria.insertData(sig_tablename+y, tuple([code] + list(map(int, tmp))))
                        self.maria.commitDB()
                    except Exception as e:
                        self.maria.insertData(sig_tablename+y, tuple([code] + [-9999999, -9999999]))
                        self.maria.commitDB()
                        pass
                                
    def fnguideCollect(self):
        self._setMaria()

        cols = ['날짜', '자산', '부채', '자본', '기초현금및현금성자산', '기말현금및현금성자산',
        '유동자산계산에 참여한 계정 펼치기', '유동부채계산에 참여한 계정 펼치기',
        '매출액', '영업이익', '영업이익(발표기준)', '당기순이익']
        changed_cols = [c.replace('계산에 참여한 계정 펼치기', '') for c in cols]
        changed_cols = [c.replace('(', '') for c in changed_cols]
        changed_cols = [c.replace(')', '') for c in changed_cols]
        
        fn_tablename = 'fnguide'

        years = ['2017', '2018', '2019', '2020']
        for batch in range(20, 2500, 10):
            data = {}
            data['2017'] = []
            data['2018'] = []
            data['2019'] = []
            data['2020'] = []
            for code in self.code_list[batch:batch + 10]:
                df = self.getFN(code)
                if df is None:
                    self.writeError('error_fnguide', code, 'No page')
                    continue
                try:
                    fs = df.columns[0]  # 연결 or 개별?
                    df = df.rename(columns={df.columns[0]: "날짜"})[cols]
                    df['FS'] = [fs] * df.shape[0]
                    df['GB'] = [date[-2:] for date in list(df['날짜'])]
                    df.columns = changed_cols + ['FS', 'GB']
                    for i, y in enumerate(years):
                        data[y].append([code] + list(df.iloc[i]))
                except:
                    self.writeError('error_fnguide', code, 'No cols')
                    continue
            stock_amount_dates = ['20171228', '20181228', '20191230', '20201230', '20200929']

            for i, y in enumerate(years[:-1]):
                data[y] = pd.DataFrame(data[y])
                data[y].columns = ['티커'] + changed_cols + ['FS', 'GB']
                tmp = data[y][data[y]['GB'] == '12']
                tmp = tmp.set_index('티커')[changed_cols[1:] + ['FS', 'GB']]
                data['stock_amount_{}'.format(y)] = stock.get_market_cap_by_ticker(stock_amount_dates[i])[['상장주식수']]
                tmp = pd.merge(tmp, data['stock_amount_{}'.format(y)], left_index=True, right_index=True)[changed_cols[1:] + ['상장주식수', 'FS', 'GB']].fillna(-9999999)
                for j in range(tmp.shape[0]):
                    self.maria.insertData('{}{}'.format(fn_tablename, y), tuple(
                        [tmp.index[j]] + list(map(int, list(tmp.iloc[j][tmp.columns[:-2]]))) + [
                            tmp['FS'].iloc[j][-3:-1]] + [tmp['GB'].iloc[j]]))
                    self.maria.commitDB()
            i += 1
            y = years[i]
            data[y] = pd.DataFrame(data[y])
            data[y].columns = ['티커'] + changed_cols + ['FS', 'GB']
            tmp = data[y][data[y]['GB'] == '12']
            tmp = tmp.set_index('티커')[changed_cols[1:] + ['FS', 'GB']]
            data['stock_amount_{}'.format(y)] = stock.get_market_cap_by_ticker(stock_amount_dates[i])[['상장주식수']]
            tmp = pd.merge(tmp, data['stock_amount_{}'.format(y)], left_index=True, right_index=True)[changed_cols[1:] + ['상장주식수', 'FS', 'GB']].fillna(-9999999)
            for j in range(tmp.shape[0]):
                self.maria.insertData('{}{}'.format(fn_tablename, y), tuple(
                    [tmp.index[j]] + list(map(int, list(tmp.iloc[j][tmp.columns[:-2]]))) + [
                        tmp['FS'].iloc[j][-3:-1]] + [tmp['GB'].iloc[j]]))
                self.maria.commitDB()
            
            tmp = data[y][data[y]['GB'] == '09']
            tmp = tmp.set_index('티커')[changed_cols[1:] + ['FS', 'GB']]
            data['stock_amount_{}'.format(y)] = stock.get_market_cap_by_ticker(stock_amount_dates[i+1])[['상장주식수']]
            tmp = pd.merge(tmp, data['stock_amount_{}'.format(y)], left_index=True, right_index=True)[changed_cols[1:] + ['상장주식수', 'FS', 'GB']].fillna(-9999999)
            for j in range(tmp.shape[0]):
                self.maria.insertData('{}{}'.format(fn_tablename, y), tuple(
                    [tmp.index[j]] + list(map(int, list(tmp.iloc[j][tmp.columns[:-2]]))) + [
                        tmp['FS'].iloc[j][-3:-1]] + [tmp['GB'].iloc[j]]))
                self.maria.commitDB()

    def getFN(self, code):
        try:
            df = pd.read_html(self.getHtmlFnguide(code, 0))
            df = pd.concat([df[0][df[0].columns[:5]], df[2], df[4]]).transpose().reset_index(drop=False)
            df.columns = df.iloc[0]
            df = df.drop(0).reset_index(drop=True)
            return df
        except:
            return None

    def getHtmlFnguide(self, ticker, gb):
        """
        :param ticker: 종목코드
        :param gb: 데이터 종류 (0 : 재무제표, 1 : 재무비율, 2: 투자지표, 3: snapshot)
        :return:
        """
        url = []

        url.append(
            "https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A" + ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701")
        url.append(
            "https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A" + ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701")
        url.append(
            "https://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&gicode=A" + ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=105&stkGb=701")
        url.append(
            'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB=&NewMenuID=11&stkGb=701'.format(ticker))
        if gb > 3:
            return None

        url = url[gb]
        try:

            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            html_text = urlopen(req).read()

        except AttributeError as e:
            print(e.args)
            return None

        return html_text

    def writeError(self, filename, code, error):
        with open('{}.csv'.format(filename), 'a', newline='') as csvfile:
            spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            if type(error) == list:
                spamwriter.writerow([code] + error)
            else:
                spamwriter.writerow([code, error])

    def getDoneList(self, tablename):
        if self.is_maria == False:
            self._setMaria()
        if self.is_maria:
            return list(self.maria.showData('select * from {}'.format(tablename))['종목코드'])
        else:
            print('maria db 연동이 되어있지 않습니다.')
            return

    def computeData(self, fnguide_tablename, signal_tablename, mh_tablename, jb_tablename, dg_tablename, res_tablename):
        self._setMaria()
        data = {}
        std_date = '20201116'
        today = '20210305'
        res_tablename = res_tablename+std_date

        years = ['2017', '2018', '2019', '2020']

        for y in years:
            data['fnguide{}'.format(y)] = self.maria.showData('select * from {}{}'.format(fnguide_tablename, y))
        for y in years[:-1]:
            data['new_signal{}'.format(y)] = self.maria.showData('select * from {}{}'.format(signal_tablename, y))
        data['dart_mh'] = self.maria.showData('select * from {}'.format(mh_tablename))
        data['dart_jb'] = self.maria.showData('select * from {}'.format(jb_tablename))

        # 사업보고서가 발표 된다면 사용하지 않음
        data['dart_dg_3q'] = self.maria.showData('select * from {}'.format(dg_tablename))
        ####################################
        
        df = data['fnguide{}'.format(years[3])][['종목코드', '자산', '부채', '자본', '기말현금및현금성자산', '매출액', '영업이익발표기준', '당기순이익', 'GB']]
        df = df.rename(columns={'자산': '자산총계{}'.format(years[3]), '부채': '부채총계{}'.format(years[3]), '자본': '자본총계{}'.format(years[3]), '기말현금및현금성자산': '현금성자산', '매출액': '매출액{}'.format(years[3]), '영업이익발표기준': '영업이익{}'.format(years[3]), '당기순이익': '당기순이익{}'.format(years[3])})

        tmp = data['fnguide{}'.format(years[0])][['종목코드', '당기순이익', '영업이익발표기준', '자산']]
        tmp = tmp.rename(columns={'당기순이익': '당기순이익{}'.format(years[0]), '영업이익발표기준': '영업이익{}'.format(years[0]),'자산': '자산총계{}'.format(years[0])})
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['fnguide{}'.format(years[1])][['종목코드', '당기순이익', '영업이익발표기준', '자본', '자산', '부채', '유동자산', '유동부채', '매출액']]
        tmp = tmp.rename(columns={'당기순이익': '당기순이익{}'.format(years[1]), '영업이익발표기준': '영업이익{}'.format(years[1]), '자본': '자본총계{}'.format(years[1]), '자산': '자산총계{}'.format(years[1]), '부채': '부채총계{}'.format(years[1]), '유동자산': '유동자산{}'.format(years[1]), '유동부채': '유동부채{}'.format(years[1]), '매출액': '매출액{}'.format(years[1])})
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['fnguide{}'.format(years[2])][['종목코드', '당기순이익', '영업이익발표기준', '자본', '자산', '부채', '유동자산', '유동부채', '매출액']]
        tmp = tmp.rename(columns={'당기순이익': '당기순이익{}'.format(years[2]), '영업이익발표기준': '영업이익{}'.format(years[2]), '자본': '자본총계{}'.format(years[2]), '자산': '자산총계{}'.format(years[2]), '부채': '부채총계{}'.format(years[2]), '유동자산': '유동자산{}'.format(years[2]), '유동부채': '유동부채{}'.format(years[2]), '매출액': '매출액{}'.format(years[2])})
        df = pd.merge(df, tmp, on=['종목코드'])

        # 사업보고서가 발표 된다면 해당년도로 사용
        tmp = data['new_signal{}'.format(years[2])][['종목코드', '단기차입금', '장기차입금']]
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['dart_mh'][['종목코드', 'mh{}'.format(years[2])]]
        tmp = tmp.rename(columns={'mh{}'.format(years[2]): '무형자산'})
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['dart_jb'][['종목코드', 'jb{}'.format(years[2])]]
        tmp = tmp.rename(columns={'jb{}'.format(years[2]): '자본금'})
        df = pd.merge(df, tmp, on=['종목코드'])
        #############################################

        df_An = df[df['GB'] == '12']
        df_3q = df[df['GB'] == '09'].rename(columns={'당기순이익{}'.format(years[3]): '당기순이익{}_3Q'.format(years[3])})

        tmp = data['dart_dg_3q'][['종목코드', 'dg{}_3Q'.format(years[1]), 'dg{}_3Q'.format(years[2])]]
        tmp = tmp.rename(columns={'dg{}_3Q'.format(years[1]): '당기순이익{}_3Q'.format(years[1]), 'dg{}_3Q'.format(years[2]): '당기순이익{}_3Q'.format(years[2])})
        df_3q = pd.merge(df_3q, tmp, on=['종목코드'])

        tmp = df_3q[['종목코드', '당기순이익{}_3Q'.format(years[1]), '당기순이익{}_3Q'.format(years[2]), '당기순이익{}_3Q'.format(years[3])]]
        tmp['rate{}'.format(years[2])] = (tmp['당기순이익{}_3Q'.format(years[2])] - tmp['당기순이익{}_3Q'.format(years[1])]) / tmp['당기순이익{}_3Q'.format(years[1])]
        tmp['rate{}'.format(years[3])] = (tmp['당기순이익{}_3Q'.format(years[3])] - tmp['당기순이익{}_3Q'.format(years[2])]) / tmp['당기순이익{}_3Q'.format(years[2])]
        tmp['당기순이익{}'.format(years[3])] = tmp['당기순이익{}_3Q'.format(years[2])] * (1 + (tmp['rate{}'.format(years[2])] + tmp['rate{}'.format(years[3])]) / 2)

        df_3q = pd.merge(df_3q, tmp[['종목코드', '당기순이익{}'.format(years[3])]], on=['종목코드'])
        df_3q = df_3q[list(df_An.columns)]

        df = pd.concat([df_An, df_3q])

        stock_amount_dates = ['20190329', '20200327', std_date]
        for y in stock_amount_dates:
            data['stock_amount_{}'.format(y)] = stock.get_market_cap_by_ticker(y)[['상장주식수', '시가총액']]

        tmp = data['stock_amount_{}'.format(stock_amount_dates[0])].reset_index(drop=False)
        tmp.columns = ['종목코드', '상장주식수{}'.format(years[1]), '시가총액{}'.format(years[1])]
        tmp['상장주식수{}'.format(years[1])] = tmp['상장주식수{}'.format(years[1])] / 100000000
        tmp['시가총액{}'.format(years[1])] = tmp['시가총액{}'.format(years[1])] / 100000000
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['stock_amount_{}'.format(stock_amount_dates[1])].reset_index(drop=False)
        tmp.columns = ['종목코드', '상장주식수{}'.format(years[2]), '시가총액{}'.format(years[2])]
        tmp['상장주식수{}'.format(years[2])] = tmp['상장주식수{}'.format(years[2])] / 100000000
        tmp['시가총액{}'.format(years[2])] = tmp['시가총액{}'.format(years[2])] / 100000000
        df = pd.merge(df, tmp, on=['종목코드'])

        tmp = data['stock_amount_{}'.format(stock_amount_dates[2])].reset_index(drop=False)
        tmp.columns = ['종목코드', '상장주식수{}'.format(years[3]), '시가총액{}'.format(years[3])]
        tmp['상장주식수{}'.format(years[3])] = tmp['상장주식수{}'.format(years[3])] / 100000000
        tmp['시가총액{}'.format(years[3])] = tmp['시가총액{}'.format(years[3])] / 100000000
        #df = pd.merge(df, tmp, on=['종목코드'])
        df = pd.merge(df, tmp[['종목코드', '시가총액{}'.format(years[3])]], on=['종목코드'])

        tmp = self.maria.showData('select * from stocks_202103')
        tmp['상장주식수{}'.format(years[3])] = (tmp['보통주'] + tmp['우선주']) / 100000000
        df = pd.merge(df, tmp[['종목코드', '상장주식수{}'.format(years[3])]], on=['종목코드'])
        

        for y in years[1:]:
            df['EPS{}'.format(y)] = df['당기순이익{}'.format(y)] / df['상장주식수{}'.format(y)]


        df1 = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date)['종가'])
        df1['Size'] = [0] * df1.shape[0]
        df2 = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date, market='KOSDAQ')['종가'])
        df2['Size'] = [1] * df2.shape[0]
        df3 = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date, market='KONEX')['종가'])
        df3['Size'] = [2] * df3.shape[0]
        price = pd.concat([df1, df2, df3])
        price = price.reset_index(drop=False).rename(columns={'티커': '종목코드', '종가': '주식가격'})
        df = pd.merge(df, price, on='종목코드')


        df['순자산가치'] = (df['자본총계{}'.format(years[3])] - df['무형자산']) / df['상장주식수{}'.format(years[3])]
        df['순손익가치'] = ((df['EPS{}'.format(years[1])] + df['EPS{}'.format(years[2])]*2 + df['EPS{}'.format(years[3])]*3)/6)*10
        df['본질가치'] = (df['순자산가치'] + (df['순손익가치']*1.5))/2.5
        df['보충적가치'] = (df['순손익가치']*2 + df['순자산가치']*3) / 5

        df['주당기업가치'] = (df['상장주식수{}'.format(years[3])]*df['주식가격']+df['단기차입금']+df['장기차입금']-df['현금성자산'])/df['상장주식수{}'.format(years[3])]

        df['주당가치분석'] = (df['본질가치'] + df['주당기업가치']) / 2

        cols = list(df.columns)
        cols.remove('GB')

        df = df[cols]
        print(df.columns)
        print(len(list(df.columns)))
        self.maria.createTable(res_tablename, list(df.columns), ['VARCHAR(20)'] + ['FLOAT']*len(cols))
        for i in range(df.shape[0]):
            try:
                self.maria.insertData(res_tablename, tuple(list(df.iloc[i])))
                self.maria.commitDB()
            except:
                continue

    def analyzeData(self, anal_type='a', res_tablename='dart2019', std_date='20201116', today='20210305', dept_rate=None):
        '''
        anal_type
        a: 기업분석 > 기준가 
        b: 가치분석 < 기업가치
        c: 영업이익 2년 연속 상승
        d: 부채비율 100% 이하
        e: 최소 주당가치분석 < 최대 주당기업가치
        g: 현재가 추가 데이터
        orgin: 원래 데이터
        '''
        years = ['2017', '2018', '2019', '2020']
        if self.is_maria == False:
            self._setMaria()
        
        df = self.maria.showData('select * from {}'.format(res_tablename))
        if anal_type == 'a':
            std_date = std_date
            df1 = stock.get_market_ohlcv_by_ticker(std_date)['종가']
            df2 = stock.get_market_ohlcv_by_ticker(std_date, market='KOSDAQ')['종가']
            df3 = stock.get_market_ohlcv_by_ticker(std_date, market='KONEX')['종가']
            price_df = pd.concat([df1, df2, df3]).reset_index(drop=False).rename(columns={'티커': '종목코드', '종가': '기준가격'})
            a_df = pd.merge(df[['종목코드', '주당가치분석']], price_df, on=['종목코드'])
            a_df = a_df[a_df['주당가치분석'] > a_df['기준가격']][['종목코드', '기준가격']]
            return pd.merge(a_df, df, on=['종목코드']).reset_index(drop=True)
        elif anal_type == 'b':
            return df[df['주당기업가치'] > df['주당가치분석']].reset_index(drop=True)
        elif anal_type == 'c':
            c_df = pd.DataFrame()
            c_df['종목코드'] = df['종목코드']
            c_df['{}영업이익성장률'.format(years[1])] = ((df['영업이익{}'.format(years[1])] - df['영업이익{}'.format(years[0])]) / df['영업이익{}'.format(years[0])])*100
            c_df['{}영업이익성장률'.format(years[2])] = ((df['영업이익2019'] - df['영업이익{}'.format(years[1])]) / df['영업이익{}'.format(years[1])])*100
            c_df = c_df[c_df['{}영업이익성장률'.format(years[1])] > 0]
            c_df = c_df[c_df['{}영업이익성장률'.format(years[2])] > 0]
            return pd.merge(c_df[['종목코드', '{}영업이익성장률'.format(years[1]), '{}영업이익성장률'.format(years[2])]], df, on=['종목코드']).reset_index(drop=True)
        elif anal_type == 'd':
            d_df = df[['종목코드', '자본총계{}'.format(years[3]), '부채총계{}'.format(years[3])]]
            d_df['부채비율'] = (d_df['부채총계{}'.format(years[3])] / d_df['자본총계{}'.format(years[3])]) * 100
            d_df = d_df[d_df['부채비율'] <= dept_rate][['종목코드', '부채비율']]
            d_df = d_df[d_df['부채비율'] >= 0]
            return pd.merge(d_df, df, on=['종목코드']).reset_index(drop=True)
        elif anal_type == 'e':
            df[df['본질가치'] > df['주당기업가치']].reset_index(drop=True)
        elif anal_type == 'g':
            df1 = stock.get_market_ohlcv_by_ticker(today)['종가']
            df2 = stock.get_market_ohlcv_by_ticker(today, market='KOSDAQ')['종가']
            df3 = stock.get_market_ohlcv_by_ticker(today, market='KONEX')['종가']
            price_df = pd.concat([df1, df2, df3]).reset_index(drop=False).rename(columns={'티커': '종목코드', '종가': '최신가격'})
            g_df = pd.merge(df[['종목코드', '주당가치분석']], price_df, on=['종목코드'])[['종목코드', '최신가격']]
            return pd.merge(g_df, df, on=['종목코드']).reset_index(drop=True)
        elif anal_type == 'orgin':
            return df

    def getTimeseriesData(self, analyezdData):
        from_date = '20200331'
        to_date = '20210226'
        from_date_str = '{}-{}-{}'.format(from_date[:4], from_date[4:6], from_date[6:])
        to_date_str = '{}-{}-{}'.format(to_date[:4], to_date[4:6], to_date[6:])
        price = []
        rate = []
        price.append(['날짜']+list(stock.get_market_ohlcv_by_date(from_date, to_date, "005930").index.astype('str')))
        rate.append(['날짜']+list(stock.get_market_ohlcv_by_date(from_date, to_date, "005930").index.astype('str')))

        for i in range(analyezdData.shape[0]):
            df = stock.get_market_ohlcv_by_date(from_date, to_date, analyezdData['종목코드'][i])
            df['rate'] = ((np.array(df['종가']) - df['종가'][from_date_str]) / df['종가'][from_date_str])*100

            price.append([analyezdData['종목코드'][i]]+list(stock.get_market_ohlcv_by_date(from_date, to_date, analyezdData['종목코드'][i])['종가']))
            rate.append([analyezdData['종목코드'][i]]+list(df['rate']))

        # 코스피 종목수 934
        df = fdr.DataReader('KS11', from_date, to_date)
        df['rate'] = ((np.array(df['Close']) - df['Close'][from_date_str]) / df['Close'][from_date_str])*100
        price.append(['KOSPI']+list(df['Close']))
        rate.append(['KOSPI']+list(df['rate'])) 

        # 코스닥 종목수 1,471
        df = fdr.DataReader('KQ11', from_date, to_date)
        df['rate'] = ((np.array(df['Close']) - df['Close'][from_date_str]) / df['Close'][from_date_str])*100
        price.append(['KOSDAQ']+list(df['Close']))
        rate.append(['KOSDAQ']+list(df['rate'])) 

        
        price = pd.DataFrame(price[1:], columns=price[0])
        rate = pd.DataFrame(rate[1:], columns=rate[0])

        # 지표평균
        mean_list = []
        for c in rate.columns[1:]:
            kospi = rate[rate['날짜']=='KOSPI'][c].iloc[0]
            kosdaq = rate[rate['날짜']=='KOSDAQ'][c].iloc[0]
            mean_list.append((kospi*934 + kosdaq*1471)/2405) # kospi, kosdaq, 지표평균은 빼고
        rate = rate.append(pd.Series(['KOS_MEAN']+mean_list, index=rate.columns), ignore_index=True)

        # 종목평균
        mean_list = []
        for c in rate.columns[1:]:
            mean_list.append(rate[c][:-3].mean()) # kospi, kosdaq, 지표평균은 빼고
        rate = rate.append(pd.Series(['CODE_MEAN']+mean_list, index=rate.columns), ignore_index=True)



        count_list = []
        tmp = rate.iloc[:-4]  # kospi, kosdaq, 지표평균, 종목평균은 빼고
        for c in rate.columns[1:]:
            count_list.append(tmp[tmp[c] > rate[rate[rate.columns[0]]=='KOS_MEAN'][c].iloc[0]].shape[0])
        count_list = list(np.array(count_list) / (rate.shape[0] - 4))
        rate = rate.append(pd.Series(['COUNT']+count_list, index=rate.columns), ignore_index=True)

        compare = rate.iloc[-5:]
        compare = pd.DataFrame(compare.transpose()).reset_index(drop=False)
        compare.columns = compare.iloc[0]
        compare = compare.iloc[1:]

        return [price, rate, compare]




