import OpenDartReader
from fnguide import *
from selenium import webdriver
import datetime
import time
import chromedriver_autoinstaller
from cjw_mysql import *
from tqdm import tqdm
import re


# param
real_time = True
is_quarter = True
period = '2021.03'

# dart 객체 생성
api_key = '408d906ab69144538333e71517c326ddd5b89d7e'
dart = OpenDartReader(api_key)

# fnguide 객체 생성
fnguide = Data()
stock_list = fnguide.getStockNameCodeDf()['종목코드'].tolist()
fnguide.setMaria(host='3.37.26.5', user='root', password='sa1234', db='fnguide', charset='utf8', port=1889)

maria = Maria()
host = '3.37.26.5'
user = 'root'
password = 'sa1234'
db = 'fnguide'
port = 1889
maria.setMaria(host=host, user=user, password=password, db=db, port=port)
dart_updated_code_list = maria.mariaShowData('dart_update_date')['code'].tolist()

# if real_time:
#     tablename = 'dart_update_date'
#     for curr_idx, curr_code in tqdm(enumerate(stock_list)):
#         if curr_code in dart_updated_code_list:
#             continue
#         try:
#             rept_list = dart.list(curr_code, start='2020-01-01', end='2021-12-31')
#             last_repr_nm = rept_list[rept_list['report_nm'].str.contains('분기보고서')]['report_nm'].iloc[0]
#             last_repr_gb = ''.join(re.findall("\d", last_repr_nm))
#             if last_repr_gb == period.replace('.', ''):
#                 last_repr_dt = rept_list['rcept_dt'].iloc[0]
#             else:
#                 last_repr_dt = 'None'
#         except:
#             last_repr_dt = 'None'
#         if last_repr_dt != 'None':
#             table_date = tuple([curr_code, last_repr_dt])
#             fnguide.mariaInsertData(tablename, table_date)
#             fnguide.mariaCommitDB()
#             dart_updated_code_list.append(curr_code)
#             time.sleep(0.7)

fnguide_updated_code_list = maria.mariaShowData('fnguide_update')['code'].tolist()

dowm_btn_selector = '#btnExcel'
quart_btn_selector = '#TERM > button:nth-child(3)'
search_btn_selector = '#btnSubmit'
latest_repr_nm_selector = '#grid1 > div.tbl--header.fixhead > table > thead > tr > th:nth-child(3)'
driver = fnguide.setChrome(visualize=True)

# 수동 로그인
curr_url = 'https://www.fnguide.com/Fgdd/FinIndivCompTrend#gicode=A005930&conyn=1&termgb=D&acntcode=10'
driver.get(curr_url)
time.sleep(10) # 10초 안에 로그인 해야함

curr_date = str(datetime.datetime.now().strftime('%Y-%m-%d'))
if real_time:
    loop = tqdm(dart_updated_code_list)
else:
    loop = tqdm(stock_list)

for curr_code in loop:
    if curr_code in fnguide_updated_code_list:
        continue
    while True:
        try:
            curr_url = f'https://www.fnguide.com/Fgdd/FinIndivCompTrend#gicode=A{curr_code}&conyn=1&termgb=D&acntcode=10'
            driver.get(curr_url)
            driver.refresh()

            if is_quarter:
                # 분기
                curr_quart_btn = driver.find_element_by_css_selector(quart_btn_selector)
                curr_quart_btn.click()
                time.sleep(1)
            # 조회
            curr_search_btn = driver.find_element_by_css_selector(search_btn_selector)
            curr_search_btn.click()
            time.sleep(2)
            # 가장 최근 보고서 이름 검색
            curr_latest_repr_nm = driver.find_element_by_css_selector(latest_repr_nm_selector).text
            if period in curr_latest_repr_nm:
                # 다운
                curr_down_btn = driver.find_element_by_css_selector(dowm_btn_selector)
                curr_down_btn.click()

                curr_url = f'https://www.fnguide.com/Fgdd/FinIndivCompTrend#gicode=A{curr_code}&conyn=1&termgb=D&acntcode=20'
                driver.get(curr_url)
                driver.refresh()

                if is_quarter:
                    # 분기
                    curr_quart_btn = driver.find_element_by_css_selector(quart_btn_selector)
                    curr_quart_btn.click()
                    time.sleep(1)

                # 조회
                curr_search_btn = driver.find_element_by_css_selector(search_btn_selector)
                curr_search_btn.click()
                time.sleep(2)

                # 가장 최근 보고서 이름 검색
                curr_latest_repr_nm = driver.find_element_by_css_selector(latest_repr_nm_selector).text
                if period in curr_latest_repr_nm:
                    # 다운
                    curr_down_btn = driver.find_element_by_css_selector(dowm_btn_selector)
                    curr_down_btn.click()

                    curr_url = f'https://www.fnguide.com/Fgdd/FinIndivCompTrend#gicode=A{curr_code}&conyn=1&termgb=D&acntcode=30'
                    driver.get(curr_url)
                    driver.refresh()

                    if is_quarter:
                        # 분기
                        curr_quart_btn = driver.find_element_by_css_selector(quart_btn_selector)
                        curr_quart_btn.click()
                        time.sleep(1)

                    # 조회
                    curr_search_btn = driver.find_element_by_css_selector(search_btn_selector)
                    curr_search_btn.click()
                    time.sleep(2)

                    curr_latest_repr_nm = driver.find_element_by_css_selector(latest_repr_nm_selector).text
                    if period in curr_latest_repr_nm:
                        # 다운
                        curr_down_btn = driver.find_element_by_css_selector(dowm_btn_selector)
                        curr_down_btn.click()


                        maria.mariaInsertData('fnguide_update', tuple([curr_code, curr_date]))
                        maria.mariaCommitDB()

                        fnguide_updated_code_list.append(curr_code)
            break
        except:
            pass