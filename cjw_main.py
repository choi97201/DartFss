import os
import csv
import math
from urllib.request import urlopen, Request

import OpenDartReader
from bs4 import BeautifulSoup as bs
import pandas as pd
import numpy as np
from pykrx import stock
import time

import cjw_maria

def get_html_fnguide(ticker, gb):
    """    
    :param ticker: 종목코드 
    :param gb: 데이터 종류 (0 : 재무제표, 1 : 재무비율, 2: 투자지표)
    :return: 
    """
    url=[]

    url.append("https://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?pGB=1&gicode=A" + ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=103&stkGb=701")
    url.append("https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A" + ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701")
    url.append("https://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&gicode=A"+ ticker + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=105&stkGb=701")

    if gb>2 :
        return None

    url = url[gb]
    try:

        req = Request(url,headers={'User-Agent': 'Mozilla/5.0'})
        html_text = urlopen(req).read()

    except AttributeError as e :
        return None

    return html_text

def ext_fin_fnguide_data(ticker,gb,item,n,freq="Q"):
    """
    :param ticker: 종목코드
    :param gb: 데이터 종류 (0 : 재무제표, 1 : 재무비율, 2: 투자지표)
    :param item: html_text file에서 원하는 계정의 데이터를 가져온다.
    :param n: 최근 몇 개의 데이터를 가져 올것인지
    :param freq: Y : 연간재무, Q : 분기재무    
    :return: item의 과거 데이터
    """

    html_text = get_html_fnguide(ticker, gb)

    soup = bs(html_text, 'lxml')

    d = soup.find_all(text=item)

    if(len(d)==0) :
        return None

    #재무제표면 최근 3년을 가져오고 재무비율이면 최근 4년치를 가져온다.
    nlimit =3 if gb==0 else 4

    if n > nlimit :
        return None
    if freq == 'a':
        #연간 데이터
        d_ = d[0].find_all_next(class_="r",limit=nlimit)
        # 분기 데이터
    elif freq =='q':
        d_ = d[1].find_all_next(class_="r",limit=nlimit)
    else:
        d_ = None

    try :
        data = d_[(nlimit-n):nlimit]
        v = [v.text for v in data]

    except AttributeError as e:
        return None

    return(v)

def getDataFromFN(code, col='당기순이익', date='2020/12'):
    profit_loss = pd.read_html(get_html_fnguide(code,gb=0))[0]
    if type(col) == list:
        tmp = []
        try:
            for c in col:
                tmp.append(profit_loss[profit_loss['IFRS(연결)'] == c][date].reset_index(drop=True)[0])
        except:
            for c in col:
                tmp.append(profit_loss[profit_loss['GAAP(개별)'] == c][date].reset_index(drop=True)[0])
        return tmp
                
    try:
        return profit_loss[profit_loss['IFRS(연결)'] == col][date].reset_index(drop=True)[0]
    except:
        return profit_loss[profit_loss['GAAP(개별)'] == col][date].reset_index(drop=True)[0]

def writeError(filename, code, error):
    with open('{}.csv'.format(filename), 'a', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        if type(error) == list:
            spamwriter.writerow([code] + error)
        else:
            spamwriter.writerow([code, error])

def readError(filename):
    df = pd.read_csv(filename, encoding='CP949')
    return df

def getPykrxData():
    stock_amount = stock.get_market_cap_by_ticker("20200929")[['상장주식수']]
    return stock_amount
dart = OpenDartReader('488d3a46f4b1d44b4be6197c7184603361289322')
class makeDartDB():
    def __init__(self):
        self.bs_list = ['ifrs-full_IssuedCapital',
            'ifrs-full_Liabilities',
            'ifrs-full_Equity',
            'ifrs-full_CurrentAssets',
            'ifrs-full_CurrentLiabilities',
            'ifrs-full_Assets',
            'ifrs-full_IntangibleAssetsOtherThanGoodwill',
            'ifrs-full_DeferredTaxAssets',
            'ifrs-full_DeferredTaxLiabilities',
            'ifrs-full_CashAndCashEquivalents',
            'ifrs-full_ShorttermBorrowings',
            'dart_LongTermBorrowingsGross']

        self.is_list = ['ifrs-full_ProfitLoss',
                    'ifrs-full_Revenue',
                    'dart_OperatingIncomeLoss']

        self.bs_list_ = ['자본금', '부채총계', '자본총계', '유동자산', '유동부채',
                    '자산총계', '무형자산', '이연법인세자산', '이연법인세부채',
                    '현금및현금성자산', '단기차입금', '장기차입금']
        self.is_list_ = ['당기순이익(손실)', '수익(매출액)', '영업이익']
        
        # 예외처리 순서
        # 연결재무제표(finstate_all) -> id로 찾기 -> name으로 찾기 -> 다른 fs로 가보기(finstate)
        
        # 예외처리1
        # is데이터가 없고 account_id 없음 -> account_nm으로 검색해야함
        self.is_list2 = [['당기순이익', '당기순이익(손실)'], 
                         [ '매출액', '수익(매출액)', '영업수익', '매출총이익'], 
                         ['영업이익']]
        
        # 예외처리2
        # 자본금 데이터 대신에 보통주자본금 데이터가 있음
        
        # 예외처리3
        # 자본금의 account_id가 없음
        
        # 예외처리4
        # 무형자산이 다르게 표기되어있음
        
        # 예외처리5
        # 매출액이 다르게 표기되어있음 
        
        # 예외처리6
        # 당기순이익없음 fn가이드 이용
        
        # 예외처리7
        # 영업이익 id가 없음
        
        # 예외처리8
        # 영업이익이 IS에 없음
        
        # 예외처리9
        # 매출액이 CIS에 있음
        
        # 예외처리10
        # 매출액이 영업수익으로 표기되어있음
        

    def makeData(self, code):
        self.expc = 0
        # 모기업만 해당
        fs = dart.finstate(corp=code, bsns_year=2020, reprt_code='11014')
        if fs is None:
            fs_all = dart.finstate_all(corp=code, bsns_year=2020, reprt_code='11014')
            if fs_all is None:
                self.expc = 1
                writeError('error', code, 'nonetype')
                return
            
        df = fs[fs['fs_div']=='CFS'][['account_nm','thstrm_amount']].reset_index(drop=True)
        
        # 예외처리 1
        if fs_is.empty:
            fs = dart.finstate_all(corp=code, bsns_year=2020, reprt_code='11014')[['account_nm','thstrm_amount', 'sj_nm']]
            # fs들 중 가장 먼저 해당되는 값으로 지정
            for ii, i in enumerate(self.is_list2):
                for jj, j in enumerate(i): # 당기순이익, 매출액, 영업이익
                    tmp = pd.DataFrame()
                    if j in list(fs['account_nm']):
                        tmp['account_nm'] = [j]
                        tmp['account_id'] = [self.is_list[ii]]
                        tmp['thstrm_amount'] = fs[fs['account_nm'] == j]['thstrm_amount'].reset_index(drop=True)[0]
                        df = df.append(tmp).reset_index(drop=True)
                        bigo2 = fs[fs['account_nm'] == j]['sj_nm'].reset_index(drop=True)[0]
                        writeError('bigo', code, [i[0], 'none', bigo2])
                        break
        
        # 데이터는 있지만 id 또는 nm이 다른경우
        if 'ifrs-full_IssuedCapital' not in list(df['account_id']):
            if 'dart_IssuedCapitalOfCommonStock' not in list(df['account_id']):
                df.loc[(df.account_nm == '자본금'), 'account_id'] = 'ifrs-full_IssuedCapital'
            else:
                df = df.replace('dart_IssuedCapitalOfCommonStock', 'ifrs-full_IssuedCapital')
                
        if 'ifrs-full_IntangibleAssetsOtherThanGoodwill' not in list(df['account_id']):
            if 'dart_GoodwillGross' not in list(df['account_id']):
                df = df.replace('dart_OtherIntangibleAssetsGross', 'ifrs-full_IntangibleAssetsOtherThanGoodwill')
            else:
                df = df.replace('dart_GoodwillGross', 'ifrs-full_IntangibleAssetsOtherThanGoodwill')
                    
        if 'ifrs-full_Revenue' not in list(df['account_id']):
            if 'ifrs-full_GrossProfit' in list(df['account_id']):
                df = df.replace('ifrs-full_GrossProfit','ifrs-full_Revenue')
        
        # 데이터가 없는 경우
        fs = dart.finstate(corp=code, bsns_year=2020, reprt_code='11014')[['account_nm','thstrm_amount', 'fs_nm', 'sj_nm']]
        # fs들 중 가장 먼저 해당되는 값으로 지정
        for ii, i in enumerate(self.is_list2):
            if self.is_list[ii] not in list(df['account_id']):
                for jj, j in enumerate(i): # 당기순이익, 매출액, 영업이익
                    tmp = pd.DataFrame()
                    if j in list(fs['account_nm']):
                        tmp['account_nm'] = [j]
                        tmp['account_id'] = [self.is_list[ii]]
                        tmp['thstrm_amount'] = fs[fs['account_nm'] == j]['thstrm_amount'].reset_index(drop=True)[0]
                        df = df.append(tmp).reset_index(drop=True)
                        bigo1 = fs[fs['account_nm'] == j]['fs_nm'].reset_index(drop=True)[0]
                        bigo2 = fs[fs['account_nm'] == j]['sj_nm'].reset_index(drop=True)[0]
                        writeError('bigo', code, [i[0], bigo1, bigo2])
                        break
                if tmp.empty:
                    tmp = pd.DataFrame()
                    tmp['account_nm'] = [j]
                    tmp['account_id'] = [self.is_list[ii]]
                    tmp['thstrm_amount'] = int(getDataFromFN(code, i[0])) * 100000000
                    df = df.append(tmp).reset_index(drop=True)
                    writeError('bigo', code, [i[0], 'none', 'FNguide'])
    
        return df
        
    def getData(self, code, df):
        # need 데이터에 해당하는 인덱스만 추출
        self.idx_list = {}
            
        for i in range(df.shape[0]):
            if df['account_id'][i] in self.bs_list+self.is_list:
                self.idx_list[df['account_id'][i]] = i
        tmp = []
        for i in self.bs_list+self.is_list:
            try:
                tmp.append(df.iloc[self.idx_list[i]])
            except:
                continue
        
        df = pd.DataFrame(tmp).reset_index(drop=True)

        # 포함하지 못한 need 데이터 인덱스 추출
        error_list = []
        for i in range(len(self.bs_list+self.is_list)):
            if (self.bs_list+self.is_list)[i] not in list(df['account_id']):
                error_list.append(i)

        # 모든 need 데이터를 포함하는 경우
        if not error_list:
            tmp = list(df['thstrm_amount'])
            tmp_ = []
            for t in tmp:
                if t == '' or t == '-': ## 공백데이터
                    tmp_.append(1)
                else:
                    tmp_.append(np.int64(str(t).replace(',','')))
            maria.insertData('done', tuple([code] + tmp_))
            maria.commitDB()
            return ['A'+code] + tmp_

        # 모든 need 데이터를 포함하지 못한 경우
        else:
            # 못구한 데이터 2
            errors = [df[df['account_id']==i]['thstrm_amount'].iloc[0] if i in list(df['account_id']) else 2 for i in self.bs_list+self.is_list]
            tmp_ = []
            for t in errors:
                if t == '' or t == '-': ## 공백데이터 1
                    tmp_.append(1)
                else:
                    tmp_.append(np.int64(str(t).replace(',','')))
            
            maria.insertData('notyet', tuple([code] + tmp_))
            maria.commitDB()
            
            return ['A'+code] + tmp_

    def play(self, code):
        df = self.makeData(code)
        if self.expc == 0:
            self.getData(code, df)
    




#eps = stock.get_market_fundamental_by_ticker("20200929")[['EPS']]
