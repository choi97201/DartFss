import os
import pandas as pd
from pykrx import stock
import pyautogui
import time
import numpy as np
from tqdm import tqdm
import pymysql
from selenium import webdriver
import shutil
import FinanceDataReader as fdr
import matplotlib.pyplot as plt
import chromedriver_autoinstaller
from selenium import webdriver


class Data:
    def __init__(self):
        self.is_chrome = False
        self.path = None
        self.connect = None
        self.cur = None
    
    def setMaria(self, host='3.37.26.5', user='root', password='sa1234', db='fnguide', charset='utf8', port=1889):
        self.connect = pymysql.connect(host=host, user=user, password=password,db=db, charset=charset, port=port)
        self.cur = self.connect.cursor()
        return
    
    def mariaInsertData(self, tablename, data):
        sql = "insert into {} values {};".format(tablename, data)
        try:
            self.cur.execute(sql)
        except Exception as e:
            if not 'Duplicate' in str(e.args):
                print(e.args)
        return

    def mariaCommitDB(self):
        self.connect.commit()
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
            return None
    
    def mariaSql(self, sql):
        self.cur.execute(sql)
        self.cur.fetchall()
        return

    def mariaShowTables(self, sql=None):
        if sql is None:
            self.cur.execute('show tables')
            df = self.cur.fetchall()
            field_names = [i[0] for i in self.cur.description]
            df = pd.DataFrame(df, columns=field_names)
        else:
            try:
                self.cur.execute(sql)
                df = self.cur.fetchall()
                field_names = [i[0] for i in self.cur.description]
                df = pd.DataFrame(df, columns=field_names)
            except Exception as e:
                df = None
        return df

    def mariaCreateTable(self, tablename, columns, columns_type):
        sql = "CREATE TABLE {} ({} {} PRIMARY KEY".format(tablename, columns[0], columns_type[0])
        try:
            for i in range(1, len(columns)):
                sql += ", {} {}".format(columns[i], columns_type[i])
            sql += ');'
            self.cur.execute(sql)
        except Exception as e:
            pass
        return

    def getFnCsvFile(self, filename):
        df = pd.read_csv(self.path + '/' + filename)
        return df
    
    def modifyFnCsvFile(self, df):
        df.columns = df.iloc[7]
        df = df.drop(7).reset_index(drop=True)
        df = df.iloc[7:].reset_index(drop=True)
        cols_list = []
        col1, col2, col3, col4, col5, col6, col7 = None, None, None, None, None, None, None
        for c in df[df.columns[0]]:
            if '  ' not in c:
                col1 = c.replace(' ', '')
                cols_list.append(col1)
            elif '    ' not in c:
                col2 = col1 + '_' + c.replace(' ', '')
                cols_list.append(col2)
            elif '      ' not in c:
                col3 = col2 + '_' + c.replace(' ', '')
                cols_list.append(col3)
            elif '        ' not in c:
                col4 = col3 + '_' + c.replace(' ', '')
                cols_list.append(col4)
            elif '          ' not in c:
                col5 = col4 + '_' + c.replace(' ', '')
                cols_list.append(col5)
            elif '            ' not in c:
                col6 = col5 + '_' + c.replace(' ', '')
                cols_list.append(col6)
            elif '              ' not in c:
                col7 = col6 + '_' + c.replace(' ', '')
                cols_list.append(col7)
            elif '                ' not in c:
                col8 = col7 + '_' + c.replace(' ', '')
                cols_list.append(col8)

        df[df.columns[0]] = cols_list
        return df
    
    def getFnCsvStockName(self, df):
        return df[df.columns[0]].iloc[2].replace('종목명 : ', '')

    def getFnCsvFileList(self):
        file_list = os.listdir(self.path)
        tmp = []
        for f in file_list:
            if '.csv' in f:
                tmp.append(f)
        return tmp
    
    def checkFnCsvFileCommon(self):
        tmp = []
        file_list = self.getFnCsvFileList()
        for f in file_list:
            df = self.getFnCsvFile(f)
            df = self.modifyFnCsvFile(df)
            tmp.append(df.columns[2][:7])
        return tmp
    
    def setFnCsvPath(self, path):
        self.path = path
        return
    
    def renameFnCsvFile(self, filename, stock_name):
        if stock_name == '':
            return
        os.rename(self.path+'/'+filename, self.path+'/'+stock_name+'.csv')
        return
    
    def modifyFnCsvFiles(self):
        self.setFnCsvPath(path='C:/Users/choi97201/Downloads/tmp_csv')
        file_list = self.getFnCsvFileList()
        for f in file_list:
            try:
                self.renameFnCsvFile(filename=f, stock_name=self.getFnCsvStockName(self.getFnCsvFile(filename=f)))
            except:
                pass
        return
    
    def getJaemusangtaepyoWishList(self, wish_list):
        wish_list['자본'] = 'jabonchonggye'
        wish_list['자산'] = 'jasanchonggye'
        wish_list['부채'] = 'buchaechonggye'
        wish_list['자산_유동자산'] = 'yudongjasan'
        wish_list['자산_비유동자산'] = 'biyudongjasan'
        wish_list['부채_유동부채'] = 'yudongbuchae'
        wish_list['*발행한주식총수_지배기업주주지분_자본금'] = 'jabongeum'
        wish_list['부채_유동부채_단기차입금'] = 'dangichaibgeum'
        wish_list['부채_비유동부채_장기차입금'] = 'janggichaibgeum'
        wish_list['자산_비유동자산_무형자산'] = 'muhyeongjasan'
        wish_list['자산_유동자산_현금및현금성자산'] = 'hyeongeumseongjasan'
        wish_list['자산_비유동자산_이연법인세자산'] = 'iyeonbeobinsejasan'
        wish_list['부채_비유동부채_이연법인세부채'] = 'iyeonbeobinsebuchae'
        wish_list['*발행한주식총수_지배기업주주지분'] = 'jibaejujujibun'
        wish_list['*발행한주식총수_*보통주'] = 'botongju'
        wish_list['*발행한주식총수_*우선주'] = 'useonju'
        return

    def getPogwalsoniggyesanseoWishList(self, wish_list):
        wish_list['매출액(수익)'] = 'maechulaeg' 
        wish_list['영업수익'] = 'maechulaeg' 
        wish_list['영업이익(손실)'] = 'yeongeobiig'
        wish_list['영업손익'] = 'yeongeobiig'
        wish_list['금융원가_이자비용'] = 'ijabiyong'
        wish_list['영업비용_이자비용'] = 'ijabiyong'
        wish_list['법인세비용'] = 'beobinsebiyong'
        wish_list['당기순이익(손실)'] = 'danggisuniig'
        wish_list['당기순손익'] = 'danggisuniig'
        wish_list['판매비와관리비_유무형자산상각비_감가상각비'] = 'gamgasanggagbi'
        wish_list['영업비용_재산관리비_감가상각비'] = 'gamgasanggagbi' 
        wish_list['매출총이익(손실)'] = 'maechulchongiig'
        wish_list['영업손익'] = 'maechulchongiig'
        wish_list['(당기순손익귀속)지배기업주주지분'] = 'jibaejujusuniig'
        wish_list['당기순손익_지배주주지분당기순손익'] = 'jibaejujusuniig' 
        return
    
    def getJaemubiyulWishList(self, wish_list):
        wish_list['재무비율_수익성_투하자본수익률_ROE(지배주주순이익)'] = 'roe'
        wish_list['상대가치지표_주가배수_수정PER'] = 'per'
        wish_list['상대가치지표_주가배수_수정PBR'] = 'pbr'
        wish_list['상대가치지표_주가배수_수정PCR'] = 'pcr'
        wish_list['상대가치지표_주당지표_EPS'] = 'eps'
        wish_list['재무비율_수익성_투하자본수익률_ROA(당기순이익)'] = 'roa'
        return

    def getHyeongeumheuleumpyoWishList(self, wish_list):
        wish_list['영업활동으로인한현금흐름(간접법)'] = 'yeongeobhyeongeumheuleum'
        wish_list['투자활동으로인한현금흐름_(투자활동으로인한현금유출액)_무형자산의증가'] = 'muhyeongjasanjeungga'
        wish_list['투자활동으로인한현금흐름_(투자활동으로인한현금유출액)_유형자산의증가'] = 'yuhyeongjasanjeungga'
        wish_list['재무활동으로인한현금흐름_배당금지급(-)'] = 'baedanggeum'
        return 
    
    def insertFnData(self, gb, is_quarter):
        print(gb)
        if is_quarter:
            self.setFnCsvPath('C:/Users/choi97201/Downloads/quarter/{}'.format(gb))
        else:
            self.setFnCsvPath('C:/Users/choi97201/Downloads/annual/{}'.format(gb))
        file_list = self.getFnCsvFileList()
        stock_name_df = self.getStockNameCodeDf()
        wish_list = {}
        if gb == 'jaemusangtaepyo':
            self.getJaemusangtaepyoWishList(wish_list)
        elif gb == 'pogwalsoniggyesanseo':
            self.getPogwalsoniggyesanseoWishList(wish_list)
        elif gb == 'jaemubiyul':
            self.getJaemubiyulWishList(wish_list)
        elif gb == 'hyeongeumheuleumpyo':
            self.getHyeongeumheuleumpyoWishList(wish_list)
        else:
            return 
        for f in tqdm(range(len(file_list))):
            stock_name = file_list[f].replace('.csv', '')[7:]
            date = file_list[f][:7]
            try:
                stock_code = stock_name_df[stock_name_df['회사명'] == stock_name]['종목코드'].iloc[0]
            except:
                stock_code = '999999'
            curr_file_nm = file_list[f]
            curr_csv_file = self.getFnCsvFile(filename=curr_file_nm)
            df = self.modifyFnCsvFile(df=curr_csv_file)

            for c in list(wish_list.keys()):
                if is_quarter:
                    tablename = 'fnguideQ_{}_{}'.format(wish_list[c], date)
                    dates = [c[:7].replace('.', '_')+'q' for c in df.columns[2:]]
                else:
                    tablename = 'fnguide_{}_{}'.format(wish_list[c], date)
                    dates = [c[:7].replace('.', '_') for c in df.columns[2:]]
                cols = ['code', 'name'] + dates
                dtypes = ['VARCHAR(20)', 'VARCHAR(50)'] + ['FLOAT']*len(dates)
                empty_check = self.mariaShowData(tablename)
                if empty_check is None:
                    self.mariaCreateTable(tablename, cols, dtypes)
                else:
                    if stock_code == '999999':
                        if stock_name in list(empty_check['code']):
                            continue
                    else:
                        if stock_code in list(empty_check['code']):
                            continue
                
                category_name = df.columns[0]
                try:
                    selected_df = df[df[category_name] == c].reset_index(drop=True)
                    unit = selected_df['단위'].iloc[0]
                    if unit == '천원':
                        mul = 1000
                    elif unit == '주':
                        mul = 1
                    elif unit == '%':
                        mul = 0.01
                    elif unit == '배':
                        mul = 1
                    elif unit == '원':
                        mul = 1
                    else:
                        print(unit)

                    data_list = list(selected_df[selected_df.columns[2:]].iloc[0])

                    if stock_code == '999999':
                        data_list = [stock_name, stock_code] + [float(d.replace(',',''))*mul if str(d) != 'nan' and str(d) != 'N/A(IFRS)' and str(d) != '완전잠식' else -123456 for d in data_list]
                    else:
                        data_list = [stock_code, stock_name] + [float(d.replace(',',''))*mul if str(d) != 'nan' and str(d) != 'N/A(IFRS)' and str(d) != '완전잠식' else -123456 for d in data_list]
                    if len(data_list) < len(list(empty_check.columns)):
                        data_list = data_list + [-123456]*(len(list(empty_check.columns))-len(data_list))
                except:
                    continue

                self.mariaInsertData(tablename, tuple(data_list))
            self.mariaCommitDB()
        return
    
    def getStockNameCodeDf(self):
        url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
        kosdaq = pd.read_html(url + "?method=download&marketType=kosdaqMkt")[0]
        kospi = pd.read_html(url + "?method=download&marketType=stockMkt")[0]
        kosdaq.종목코드 = kosdaq.종목코드.astype(str).apply(lambda x: x.zfill(6))
        kospi.종목코드 = kospi.종목코드.astype(str).apply(lambda x: x.zfill(6))

        kosdaq['Market'] = 'kosdaq'
        kospi['Market'] = 'kospi'
        orgin_df = pd.concat([kospi[['종목코드', '회사명', 'Market']], kosdaq[['종목코드', '회사명', 'Market']]]).reset_index(
            drop=True)

        orgin_list = ['서울식품공업', '삼화콘덴서공업', '삼화전자공업', '미창석유공업', '한신기계공업',
                      '한국프랜지공업', '태양금속공업', '부산도시가스', '롯데칠성음료', '대림비앤코', '네이블커뮤니케이션즈',
                      '금호석유화학', '계룡건설산업', '티비에이치글로벌', '에스제이엠홀딩스', '에스제이엠',
                      '엔피씨', '엔에이치엔', '엘에스일렉트릭', '케이씨씨', '아이에이치큐', '디아이동일', '쌍용자동차',
                      '휴니드테크놀러지스', '화승인더스트리', '현대엘리베이터', '하이트론씨스템즈',
                      '유나이티드', '엘브이엠씨', '케이씨티시', '삼영화학공업', '한국석유공업', '동양물산기업',
                      '효성 ITX', '현대자동차', '한솔피엔에스', '한국전력공사', '한국수출포장공업', '한국단자공업',
                      '아모레퍼시픽그룹', '신세계I&C', '케이티스카이라이프', '서울도시가스', '삼화페인트공업',
                      '삼영전자공업', '교보10호기업인수목적', '에스케이바이오팜', '포스코',
                      '케이티앤지', '케이티', '유진증권', '소마젠', '삼성화재해상보험', '브릿지바이오', '네오이뮨텍'

                      ]
        modified_list = ['서울식품', '삼화콘덴서', '삼화전자', '미창석유', '한신기계',
                         '한국프랜지', '태양금속', '부산가스', '롯데칠성', '대림B&Co', '네이블',
                         '금호석유', '계룡건설', 'TBH글로벌', 'SJM홀딩스', 'SJM',
                         'NPC', 'NHN', 'LS ELECTRIC', 'KCC', 'IHQ', 'DI동일', '쌍용차',
                         '휴니드', '화승인더', '현대엘리베이', '하이트론',
                         '유나이티드제약', '엘브이엠씨홀딩스', 'KCTC', '삼영화학', '한국석유', '동양물산',
                         '효성ITX', '현대차', '한솔PNS', '한국전력', '한국수출포장', '한국단자',
                         '아모레G', '신세계 I&C', '스카이라이프', '서울가스', '삼화페인트',
                         '삼영전자', '교보10호스팩', 'SK바이오팜', 'POSCO',
                         'KT&G', 'KT', '유진투자증권', '소마젠(Reg.S)', '삼성화재', '브릿지바이오테라퓨틱스', '네오이뮨텍(Reg.S)'
                         ]
        for idx, o in enumerate(orgin_list):
            orgin_df = orgin_df.replace(o, modified_list[idx])
        return orgin_df
    
    def calValueDanggisuniig(self, df):
        print('calValueDanggisuniig()===============================================================================================')
        print('당기순이익 값을 계산하여 테이블에 추가합니다.')
        selected_cols = ['danggisuniig_year_q', 'danggisuniig_bfyear_q', 'danggisuniig_bbfyear_q', 'danggisuniig_bbbfyear_q']
        tmp = df[['code'] + selected_cols].transpose().reset_index(drop=False)
        tmp.columns = tmp.iloc[0]
        tmp = tmp.drop(0).reset_index(drop=True)
        tmp = tmp.sort_values('code').reset_index(drop=True)
        tmp.index = tmp['code']
        tmp = tmp[tmp.columns[1:]]
        tmp = tmp.pct_change().dropna().transpose().reset_index(drop=False)
        cols = tmp.columns[1:]
        tmp['rate'] = (tmp[cols[2]] + tmp[cols[1]] + tmp[cols[0]]) / 3
        tmp = tmp.rename(columns={tmp.columns[0]: 'code'})
        df['danggisuniig_bfyear'] = df['danggisuniig_year']
        df['danggisuniig_bbfyear'] = df['danggisuniig_bfyear']
        df['danggisuniig_bbbfyear'] = df['danggisuniig_bbfyear']
        df = pd.merge(tmp[['code']], df, on=['code'])
        df['danggisuniig_year'] = df['danggisuniig_bfyear'] * (1+tmp['rate'])
        return df.reset_index(drop=True)
    
    def modifyMergedTable(self, df, year, qt=None):
        cols = df.columns
        selected_cols = []
        for c in cols:
            if '{}_{}'.format(year, qt) in c:
                selected_cols.append(c)
            elif '{}_{}'.format(year-1, qt) in c:
                selected_cols.append(c)
            elif '{}_{}'.format(year-2, qt) in c:
                selected_cols.append(c)
            elif '{}_{}'.format(year-3, qt) in c:
                selected_cols.append(c)
        df = df[['code', 'name'] + selected_cols]
        
        cols = df.columns
        return cols
    
    def modifyPathFnCsvFile(self, filename):
        df = self.getFnCsvFile(filename)
        gb = df.iloc[5][df.columns[0]].split(' : ')[1]
        is_qt = True if df.iloc[4][df.columns[0]].split(' : ')[1] == '분기' else False
        date = df.iloc[7][2][:7].replace('.', '_') # 2020.12 
        if is_qt:
            if gb == '재무상태표':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/quarter/jaemusangtaepyo/{}'.format(date+filename))
            elif gb == '포괄손익계산서':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/quarter/pogwalsoniggyesanseo/{}'.format(date+filename))
            elif gb == '재무비율':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/quarter/jaemubiyul/{}'.format(date+filename))
            elif gb == '현금흐름표':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/quarter/hyeongeumheuleumpyo/{}'.format(date+filename))
            else:
                return
        else:
            if gb == '재무상태표':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/annual/jaemusangtaepyo/{}'.format(date+filename))
            elif gb == '포괄손익계산서':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/annual/pogwalsoniggyesanseo/{}'.format(date+filename))
            elif gb == '재무비율':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/annual/jaemubiyul/{}'.format(date+filename))
            elif gb == '현금흐름표':
                shutil.move(self.path +'/'+filename, 'C:/Users/choi97201/Downloads/annual/hyeongeumheuleumpyo/{}'.format(date+filename))
            else:
                return
        return
    
    def modifyPathFnCsvFiles(self):
        self.setFnCsvPath(path='C:/Users/choi97201/Downloads/tmp_csv')
        file_list = self.getFnCsvFileList()
        for curr_file_nm in file_list:
            try:
                curr_file = self.getFnCsvFile(curr_file_nm)
                curr_stock_nm = self.getFnCsvStockName(curr_file)
                self.renameFnCsvFile(curr_file_nm, curr_stock_nm)
                curr_file_nm = f'{curr_stock_nm}.csv'
                self.modifyPathFnCsvFile(curr_file_nm)
            except:
                pass
        return
    
    def modifyMergedDfColNames(self, df, year):
        print('modifyMergedDfColNames({})==============================================================================================='.format(year))
        print('열 이름을 바꿉니다')
        cols = df.columns
        selected_cols = []
        modified_cols = []
        for c in cols[2:]:
            now_cate = c.split('_')[0]
            now_year = c.split('_')[1]
            if now_year == str(year):
                selected_cols.append(c)
                modified_cols.append('{}_year'.format(now_cate))
            if now_year == str(year-1):
                selected_cols.append(c)
                modified_cols.append('{}_bfyear'.format(now_cate))
            if now_year == str(year-2):
                selected_cols.append(c)
                modified_cols.append('{}_bbfyear'.format(now_cate))
            if now_year == str(year-3):
                selected_cols.append(c)
                modified_cols.append('{}_bbbfyear'.format(now_cate))
        df = df[['code', 'name'] + selected_cols]
        df.columns = ['code', 'name'] + modified_cols
        print(selected_cols)
        print('가')
        print(modified_cols)
        print('로 바뀌었습니다.')
        return df
    
    def mergeSigachongaeg(self, df, std_date, siga_name):
        print('mergeSigachongaeg({})==============================================================================================='.format(std_date))    
        print('{} 날짜 시가총액 데이터를 테이블에 추가합니다.'.format(std_date))
        return pd.merge(df, stock.get_market_cap_by_ticker(std_date).reset_index(drop=False).rename(columns={'티커':'code', '시가총액':siga_name})[['code', siga_name]], on=['code']).reset_index(drop=True)

    def mergePrice(self, df, std_date, price_name, get_size):
        print('mergePrice({})==============================================================================================='.format(std_date))    
        print('{} 날짜 종가 데이터를 테이블에 추가합니다.'.format(std_date))
        kospi = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date)['종가'])
        kospi['Size'] = [0] * kospi.shape[0]
        kosdaq = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date, market='KOSDAQ')['종가'])
        kosdaq['Size'] = [1] * kosdaq.shape[0]
        #konex = pd.DataFrame(stock.get_market_ohlcv_by_ticker(std_date, market='KONEX')['종가'])
        #konex['Size'] = [2] * konex.shape[0]
        #price = pd.concat([kospi, kosdaq, konex])
        price = pd.concat([kospi, kosdaq])
        if get_size:
            price = price.reset_index(drop=False).rename(columns={'티커': 'code', '종가': price_name})[['code', price_name, 'Size']]
        else:
            price = price.reset_index(drop=False).rename(columns={'티커': 'code', '종가': price_name})[['code', price_name]]
        df = pd.merge(df, price, on='code')
        return df
    
    def mariaDeleteTables(self, like_sql):
        print('mariaDeleteTables()===============================================================================================')
        print('테이블을 삭제합니다.')
        like_sql = 'show tables like ' + like_sql
        tables = self.mariaShowTables(like_sql)
        tables = list(tables[tables.columns[0]])
        for t in tables:
            self.mariaSql('drop table {}'.format(t))
            print('{} 삭제.'.format(t))
        return

    def setChrome(self, visualize):
        chrome_ver = chromedriver_autoinstaller.get_chrome_version().split('.')[0]
        if not visualize:
            webdriver_options = webdriver.ChromeOptions()
            webdriver_options.add_argument('headless')
            try:
                driver = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe', options=webdriver_options)
            except:
                chromedriver_autoinstaller.install(True)
                driver = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe', options=webdriver_options)
        else:
            try:
                driver = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
            except:
                chromedriver_autoinstaller.install(True)
                driver = webdriver.Chrome(f'./{chrome_ver}/chromedriver.exe')
        return driver

    def calValueEps(self, df):
        print('calValueEps()=========================================================================================================')
        print('EPS 값을 계산하여 테이블에 추가합니다.')
        for y in ['year', 'bfyear', 'bbfyear', 'bbbfyear']:
            df['eps_{}'.format(y)] = df['jibaejujusuniig_{}'.format(y)] / (df['botongju_{}'.format(y)]+df['useonju_{}'.format(y)])
            
        return df
    
    def calValueRoe(self, df, year, qt):
        print('calValueRoe({}, {})============================================================================================'.format(year, qt))
        print('ROE 값을 계산하여 테이블에 추가합니다.')

        years = ['year', 'bfyear', 'bbfyear', 'bbbfyear']
        for i in range(len(years)-1):
            if qt == '12':
                coef = 1
                df['roe_{}'.format(years[i])] = df['jibaejujusuniig_{}'.format(years[i])]*coef / ((df['jibaejujujibun_{}'.format(years[i])] + df['jibaejujujibun_{}'.format(years[i+1])])/2)
                continue
            elif qt == '03':
                coef = 4
                tmp = self.mariaShowData(f'fnguideQ_jibaejujujibun_{year}_{qt}').rename(columns={'{}_12q'.format(year-1-i):'jibaejujujibun_tmp_{}'.format(years[i])}).replace(-123456, np.nan)[['code', 'name', 'jibaejujujibun_tmp_{}'.format(years[i])]].dropna().reset_index(drop=True)
            elif qt == '06':
                coef = 2
                tmp = self.mariaShowData(f'fnguideQ_jibaejujujibun_{year}_{qt}').rename(columns={'{}_03q'.format(year-i):'jibaejujujibun_tmp_{}'.format(years[i])}).replace(-123456, np.nan)[['code', 'name', 'jibaejujujibun_tmp_{}'.format(years[i])]].dropna().reset_index(drop=True)
            elif qt == '09':
                coef = (4/3)
                tmp = self.mariaShowData(f'fnguideQ_jibaejujujibun_{year}_{qt}').rename(columns={'{}_06q'.format(year-i):'jibaejujujibun_tmp_{}'.format(years[i])}).replace(-123456, np.nan)[['code', 'name', 'jibaejujujibun_tmp_{}'.format(years[i])]].dropna().reset_index(drop=True)
        
            df = pd.merge(df, tmp, on=['code', 'name']).reset_index(drop=True)
            df['roe_{}'.format(years[i])] = (df['jibaejujusuniig_{}_q'.format(years[i])]*coef) / ((df['jibaejujujibun_{}_q'.format(years[i])] + df['jibaejujujibun_tmp_{}'.format(years[i])])/2)

        df['roe_e'] = (1+(((((df['roe_year']/df['roe_bfyear'])-1)*2) + (((df['roe_bfyear']/df['roe_bbfyear'])-1)*1)) / 3)) * df['roe_year']
        return df

    def mergeDcfData(self, df, year):
        print('mergeDcfData({})============================================================================================'.format(year))
        print('DCF 데이터를 추가합니다.')
        gdp = self.mariaShowData('data_gdp')
        df['gdp_year'] = [gdp['{}_12_31'.format(year)].iloc[0]] * df.shape[0]
        df['gdp_bfyear'] = [gdp['{}_12_31'.format(year-1)].iloc[0]] * df.shape[0]
        wish_list = ['yeongeobhyeongeumheuleum', 'muhyeongjasanjeungga', 'yuhyeongjasanjeungga']
        for w in wish_list:
            df_w = self.mariaShowData('data_{}'.format(w))
            df_w = df_w[['code', 'name'] + ['{}_12'.format(y) for y in range(year, year-4, -1)]]
            #df_w1.columns = ['code', 'name', '{}_year'.format(w)] + ['{}_bf{}year'.format(w, i) for i in range(1, 10)]
            #df_w2.columns = ['code', 'name', '{}_year'.format(w)] + ['{}_bf{}year'.format(w, i) for i in range(1, 10)]
            df_w.columns = ['code', 'name', '{}_year'.format(w)] + ['{}_bf{}year'.format(w, i) for i in range(1, 4)]
            #df_w = pd.concat([df_w1, df_w2]).reset_index(drop=True).replace(-123456, np.nan).dropna().reset_index(drop=True)
            df_w = df_w.replace(-123456, np.nan).dropna().reset_index(drop=True)
            bf_amount = df.shape[0]
            df = pd.merge(df, df_w, on=['code', 'name'])
            print(w + '의 null 값으로 인해 데이터가 줄었습니다.')
            print('{} -> {}'.format(bf_amount, df.shape[0]))

        return df

    def calValueFcf(self, df):
        print('calValueFcf()============================================================================================')
        print('FCF 값을 계산하여 테이블에 추가합니다.')

        # 평균 수익률을 구할 때 이전 몇년의 데이터를 사용할지
        use_years = 3

        years = ['bf{}year'.format(i) for i in range(use_years, 0, -1)] + ['year']
        for y in years:
            df['fcf_{}'.format(y)] = (df['yeongeobhyeongeumheuleum_{}'.format(y)] - (df['muhyeongjasanjeungga_{}'.format(y)] + df['yuhyeongjasanjeungga_{}'.format(y)]))
        
        df['fcf_rate_average'] = [0] * df.shape[0]
        for i in range(len(years)-1):
            df['fcf_rate_average'] += ((df['fcf_{}'.format(years[i+1])] / df['fcf_{}'.format(years[i])]) - 1)
        df['fcf_rate_average'] = (df['fcf_rate_average'] / 9) - 0.05

        df['fcf_af1year'] = (1+df['fcf_rate_average']) * df['fcf_year']
        df['disc_fcf_af1year'] = df['fcf_af1year'] / ((1 + df['yogusuiglyul']))
        df['disc_fcf_10year_sum'] = df['disc_fcf_af1year']
        for i in range(2, 11):
            df['fcf_af{}year'.format(i)] = (1+df['fcf_rate_average']) * df['fcf_af{}year'.format(i-1)] 
            df['disc_fcf_af{}year'.format(i)] = df['fcf_af{}year'.format(i)] / ((1 + df['yogusuiglyul'])**(i))
            df['disc_fcf_10year_sum'] += df['disc_fcf_af{}year'.format(i)]
        df['yeonggugachi'] = df['disc_fcf_af10year'] * (1 + ((df['gdp_year']/df['gdp_bfyear']) - 1)) / (df['yogusuiglyul'] - ((df['gdp_year']/df['gdp_bfyear']) - 1))
        df['disc_yeonggugachi'] = df['yeonggugachi'] / ((1 + df['yogusuiglyul'])**(10))
        df['chongjusiggachi'] = df['disc_fcf_10year_sum'] + df['disc_yeonggugachi']
        return df

    def mergeJasaju(self, df):
        print('mergeJasaju()====================================================================================================')
        print('자사주 데이터를 테이블에 추가합니다.(추후 분기별로 가져오도록 데이터 수집해야함!!!)')
        jasaju_df = self.mariaShowData('fnguide_stocks_2021_05').rename(columns={'자사주': 'jasaju'})
        stockname_df = self.getStockNameCodeDf().rename(columns={'종목코드': 'code', '회사명': 'name'})
        new_jasaju_df = pd.merge(jasaju_df, stockname_df, on=['code']).reset_index(drop=True)[['code', 'name', 'jasaju']]
        return pd.merge(df, new_jasaju_df, on=['code', 'name']).reset_index(drop=True)
    
    def mergeYogusuiglyul(self, df):
        print('mergeYogusuiglyul()===============================================================================================')
        print('요구 수익률을 파싱합니다.')
        driver = self.setChrome(visualize=False)
        url = 'https://www.kisrating.com/ratingsStatistics/statics_spread.do'
        driver.get(url)
        html = driver.page_source
        tmp = pd.read_html(html)
        tmp = tmp[0]
        yogu = (tmp[tmp['구분'] == 'BBB-']['5년'].iloc[0]) / 100
        print('요구 수익률: ' + str(yogu))
        df['yogusuiglyul'] = [yogu] * df.shape[0]
        print('요구 수익률을 테이블에 추가합니다.')
        return df
       
    def computeDfValueEvaluation(self, df, method):
        print('computeDfValueEvaluation({})=================================================================================================='.format(method))
        print('가치 평가 지표들을 계산하여 테이블에 추가합니다.')
        if method == 'value':
            df['순자산가치'] = (df['jabonchonggye_year'] - df['muhyeongjasan_year'] - df['iyeonbeobinsejasan_year'] + df['iyeonbeobinsebuchae_year'] + df['baedanggeum_year']) / (df['botongju_year'] + df['useonju_year'])
            df['순손익가치'] = ((df['eps_bbfyear'] + df['eps_bfyear']*2 + df['eps_year']*3)/6)*10
            df['본질가치'] = (df['순자산가치'] + (df['순손익가치']*1.5))/2.5
            df['보충적가치'] = (df['순손익가치']*2 + df['순자산가치']*3) / 5
            df['주당기업가치'] = ((df['botongju_year'] + df['useonju_year'])*df['buy_price']+df['dangichaibgeum_year']+df['janggichaibgeum_year']-df['hyeongeumseongjasan_year'])/(df['botongju_year'] + df['useonju_year'])
            df['주당가치분석'] = (df['본질가치'] + df['주당기업가치']) / 2
            df['기업가치'] = df['jibaejujujibun_year'] + df['jibaejujujibun_year']*(df['roe_e'] - df['yogusuiglyul']) * (0.8 / (1 + df['yogusuiglyul']-0.8))
            df['적정주가_rim'] = df['기업가치'] / (df['botongju_year'] + df['useonju_year'] - df['jasaju'])
        elif method == 'dcf':
            df['기업가치'] = df['jibaejujujibun_year'] + df['jibaejujujibun_year']*(df['roe_e'] - df['yogusuiglyul']) * (0.8 / (1 + df['yogusuiglyul']-0.8))
            df['적정주가_rim'] = df['기업가치'] / (df['botongju_year'] + df['useonju_year'] - df['jasaju'])
            df['적정주가_dcf'] = (df['chongjusiggachi'] / (df['botongju_year'] + df['useonju_year'] - df['jasaju'])) * 0.8
        else:
            print('잘못된 method 입니다.')
            return
        return df
        
    def calScores(self, df):
        print('calScores()=========================================================================================================')
        print('점수를 계산하여 테이블에 추가합니다.')
        data = {}
        data['profit'] = ['자기자본수익률', '매출영업이익률', '매출액순이익률', '총자산이익률']
        수익성점수 = [[4, 2, 0], [8, 4, 0], [13, 7, 0]]
        수익성임계치 = [[25, 10], [5, 3], [5, 3], [5, 3]]

        data['grow'] = ['매출액증가율', '영업이익증가율', '당기순이익증가율', '자산총계증가율']
        성장성점수 = 수익성점수

        data['stable'] = ['유동비율', '부채비율', '자기자본비율']
        안정성점수1 = [[[6, 3, 0], [11, 6, 0], [18, 9, 0]], [[0, 3, 6], [0, 6, 11], [0, 9, 18]]]
        안정성점수2 = [[5, 0], [10, 0], [15, 0]]


        data['market'] = ['eps', '주당순자산가치', '주당매출액']
        시장가치비율점수 = [[50, 40, 30, 10], [25, 20, 15, 5], [25, 20, 15, 5]]
        
        years = ['bbbfyear', 'bbfyear', 'bfyear', 'year']
        
        for y in years:
            df['자기자본수익률_{}'.format(y)] = (df['danggisuniig_{}'.format(y)] / df['jabonchonggye_{}'.format(y)]) * 100
            df['매출영업이익률_{}'.format(y)] = (df['yeongeobiig_{}'.format(y)] / df['maechulaeg_{}'.format(y)]) * 100
            df['매출액순이익률_{}'.format(y)] = (df['danggisuniig_{}'.format(y)] / df['maechulaeg_{}'.format(y)]) * 100
            df['총자산이익률_{}'.format(y)] = (df['danggisuniig_{}'.format(y)] / df['jasanchonggye_{}'.format(y)]) * 100
            df['유동비율_{}'.format(y)] = (df['yudongjasan_{}'.format(y)] / df['yudongbuchae_{}'.format(y)]) * 100
            df['부채비율_{}'.format(y)] = (df['buchaechonggye_{}'.format(y)] / df['jabonchonggye_{}'.format(y)]) * 100
            df['자기자본비율_{}'.format(y)] = (df['jabonchonggye_{}'.format(y)] / df['jasanchonggye_{}'.format(y)]) * 100
            df['주당순자산가치_{}'.format(y)] = (df['jabonchonggye_{}'.format(y)] / (df['botongju_{}'.format(y)] + df['useonju_{}'.format(y)])) * 100
            df['주당매출액_{}'.format(y)] = (df['maechulaeg_{}'.format(y)] / (df['botongju_{}'.format(y)] + df['useonju_{}'.format(y)])) * 100

        for i in range(1, len(years)):
            df['매출액증가율_{}'.format(years[i])] = ((df['maechulaeg_{}'.format(years[i])] / df['maechulaeg_{}'.format(years[i-1])]) * 100) - 100
            df['영업이익증가율_{}'.format(years[i])] = ((df['yeongeobiig_{}'.format(years[i])] / df['yeongeobiig_{}'.format(years[i-1])]) * 100) - 100
            df['당기순이익증가율_{}'.format(years[i])] = ((df['danggisuniig_{}'.format(years[i])] / df['danggisuniig_{}'.format(years[i-1])]) * 100) - 100
            df['자산총계증가율_{}'.format(years[i])] = ((df['jasanchonggye_{}'.format(years[i])] / df['jasanchonggye_{}'.format(years[i-1])]) * 100) - 100
            
            for k, j in enumerate(data['profit']):
                df1 = df[df['{}_{}'.format(j, years[i])] >= 수익성임계치[k][0]]
                df2 = df[(df['{}_{}'.format(j, years[i])] >= 수익성임계치[k][1]) & (df['{}_{}'.format(j, years[i])] < 수익성임계치[k][0])]
                df3 = df[(df['{}_{}'.format(j, years[i])] < 수익성임계치[k][1])]
                df1['{}_{}_점수'.format(j, years[i])] = 수익성점수[i-1][0]
                df2['{}_{}_점수'.format(j, years[i])] = 수익성점수[i-1][1]
                df3['{}_{}_점수'.format(j, years[i])] = 수익성점수[i-1][2]
                df = pd.concat([df1, df2, df3])
            
            for j in data['grow']:
                df1 = df[df['{}_{}'.format(j, years[i])] >= 10]
                df2 = df[(df['{}_{}'.format(j, years[i])] >= 4) & (df['{}_{}'.format(j, years[i])] < 10)]
                df3 = df[(df['{}_{}'.format(j, years[i])] < 4)]
                df1['{}_{}_점수'.format(j, years[i])] = 성장성점수[i-1][0]
                df2['{}_{}_점수'.format(j, years[i])] = 성장성점수[i-1][1]
                df3['{}_{}_점수'.format(j, years[i])] = 성장성점수[i-1][2]
                df = pd.concat([df1, df2, df3])

            for k, j in enumerate(data['stable'][:-1]):
                df1 = df[df['{}_{}'.format(j, years[i])] >= 200]
                df2 = df[(df['{}_{}'.format(j, years[i])] >= 100) & (df['{}_{}'.format(j, years[i])] < 200)]
                df3 = df[(df['{}_{}'.format(j, years[i])] < 100)]
                df1['{}_{}_점수'.format(j, years[i])] = 안정성점수1[k][i-1][0]
                df2['{}_{}_점수'.format(j, years[i])] = 안정성점수1[k][i-1][1]
                df3['{}_{}_점수'.format(j, years[i])] = 안정성점수1[k][i-1][2]
                df = pd.concat([df1, df2, df3])

            j = data['stable'][-1]
            df1 = df[df['{}_{}'.format(j, years[i])] >= 8]
            df2 = df[(df['{}_{}'.format(j, years[i])] < 8)]
            df1['{}_{}_점수'.format(j, years[i])] = 안정성점수2[i-1][0]
            df2['{}_{}_점수'.format(j, years[i])] = 안정성점수2[i-1][1]
            df = pd.concat([df1, df2])

        for i, x in enumerate(data['market']):
            df1 = df[df['{}_{}'.format(x, years[3])] > df['{}_{}'.format(x, years[2])]]
            df11 = df1[df1['{}_{}'.format(x, years[2])] > df1['{}_{}'.format(x, years[1])]] 
            df111 = df11[df11['{}_{}'.format(x, years[1])] > df11['{}_{}'.format(x, years[0])]] 
            df112 = df11[df11['{}_{}'.format(x, years[1])] <= df11['{}_{}'.format(x, years[0])]] 
            df12 = df1[df1['{}_{}'.format(x, years[2])] <= df1['{}_{}'.format(x, years[1])]] 
            df2 = df[df['{}_{}'.format(x, years[3])] <= df['{}_{}'.format(x, years[2])]] 

            df111['{}_{}_점수'.format(x, years[3])] = [시장가치비율점수[i][0]] * df111.shape[0]
            df112['{}_{}_점수'.format(x, years[3])] = [시장가치비율점수[i][1]] * df112.shape[0]
            df12['{}_{}_점수'.format(x, years[3])] = [시장가치비율점수[i][2]] * df12.shape[0]
            df2['{}_{}_점수'.format(x, years[3])] = [시장가치비율점수[i][3]] * df2.shape[0]
            df = pd.concat([df111, df112, df12, df2])

        for d in list(data.keys()):
            df['{}_score'.format(d)] = [0] * df.shape[0]
            for s in data[d]:
                for y in years[1:]:
                    try:
                        df['{}_score'.format(d)] += df['{}_{}_점수'.format(s, y)]
                    except:
                        pass
                    
        df['total_score'] = df['profit_score'] + df['grow_score'] + df['stable_score'] + df['market_score']
        
        return df.reset_index(drop=True)
    
    def analyzeData(self, anal_type, res_tablename, std_date, today, profit_rate=None, dept_rate1=None, dept_rate2=None):
        '''
        anal_type
        a. 기준가격 > 주당가치분석
        b. 주당가치분석 < 기업가치
        c. 영업이익 3년 연속 상승
        d. 부채비율 n% 이하
        e. 본질가치 > 주당기업가치
        f. 현재가 < 적정주가_dcf
        g. 현재가 추가하기
        h. 현재가 < 적정주가_rim
        i. roe > 요구수익률
        orgin: 원래 데이터
        '''
        years = ['bbbfyear', 'bbfyear', 'bfyear', 'year']
        if anal_type == 'a':
            df = self.mariaShowData(res_tablename)[['code', 'name', '주당가치분석']]
            a_df = self.mergePrice(df=df, std_date=std_date, price_name='기준가격', get_size=False)
            return a_df[a_df['주당가치분석'] > a_df['기준가격']].reset_index(drop=True)
        elif anal_type == 'b':
            df = self.mariaShowData(res_tablename)[['code', 'name', '주당기업가치', '주당가치분석']]
            return df[df['주당기업가치'] > df['주당가치분석']].reset_index(drop=True)
        elif anal_type == 'c':
            c_df = self.mariaShowData(res_tablename)[['code', 'name', '영업이익증가율_{}'.format(years[1]), '영업이익증가율_{}'.format(years[2]), '영업이익증가율_{}'.format(years[3])]]
            if profit_rate is None:
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[1])] >= 0]
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[2])] >= 0]
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[3])] >= 0]
            elif profit_rate > 3:
                print('profit_rate는 3보다 클 수 없습니다.')
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[1])] >= 0]
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[2])] >= 0]
                c_df = c_df[c_df['영업이익증가율_{}'.format(years[3])] >= 0]
            else:
                for i in range(profit_rate):
                    c_df = c_df[c_df['영업이익증가율_{}'.format(years[3-i])] >= 0]
            if res_tablename.split('_')[-1] != '12':
                curr_year = res_tablename.split('_')[-2]
                tablename = f'fnguide_yeongeobiig_{int(curr_year) - 1}_12'
                tmp = self.mariaShowData(tablename)[['code', '{}_12'.format(int(curr_year)-4), '{}_12'.format(int(curr_year)-3)]]
                tmp['영업이익증가율_{}'.format(years[0])] = ((tmp['{}_12'.format(int(curr_year)-3)] / tmp['{}_12'.format(int(curr_year)-4)]) -1) * 100
                c_df = pd.merge(c_df, tmp, on=['code']) 
            return c_df.reset_index(drop=True)
        elif anal_type == 'd':
            d_df = self.mariaShowData(res_tablename)[['code', 'name', '부채비율_{}'.format(years[3])]]
            d_df = d_df[d_df['부채비율_{}'.format(years[3])] >= dept_rate1].reset_index(drop=True)
            d_df = d_df[d_df['부채비율_{}'.format(years[3])] <= dept_rate2].reset_index(drop=True)
            return d_df
        elif anal_type == 'e':
            df = self.mariaShowData(res_tablename)[['code', 'name', '본질가치', '주당기업가치']]
            return df[df['본질가치'] > df['주당기업가치']].reset_index(drop=True)
        elif anal_type == 'f':
            df = self.mariaShowData(res_tablename)[['code', 'name', '적정주가_dcf']]
            g_df = self.mergePrice(df=df, std_date=today, price_name='최신가격', get_size=False)
            return g_df[g_df['적정주가_dcf'] > g_df['최신가격']].reset_index(drop=True)
        elif anal_type == 'g':
            df = self.mariaShowData(res_tablename)[['code', 'name']]
            return self.mergePrice(df=df, std_date=today, price_name='최신가격', get_size=False)
        elif anal_type == 'h':
            df = self.mariaShowData(res_tablename)[['code', 'name', '적정주가_rim']]
            h_df = self.mergePrice(df=df, std_date=today, price_name='최신가격', get_size=False)
            return h_df[h_df['적정주가_rim'] > h_df['최신가격']].reset_index(drop=True)
        elif anal_type == 'i':
            df = self.mariaShowData(res_tablename)[['code', 'name', 'roe_e', 'yogusuiglyul']]
            i_df = self.mergePrice(df=df, std_date=today, price_name='최신가격', get_size=False)
            return i_df[g_df['roe_e'] > g_df['yogusuiglyul']].reset_index(drop=True)
        elif anal_type == 'orgin':
            df = self.mariaShowData(res_tablename)
            return df
    
    def mergeAnalyzeTable(self, data, df_list, filter_list, year, qt, save_excel):
        df = df_list[0]
        filename = filter_list[0]
        for i, d in enumerate(df_list[1:]):
            if '최신가격' in list(df.columns):
                df = pd.merge(df, d, on=['code', 'name', '최신가격'])
            else:
                df = pd.merge(df, d, on=['code', 'name'])
            filename = filename + '_' + filter_list[i+1]
            data[filename] = df
            # if save_excel:
            #     df.to_excel('{}_{}_{}.xlsx'.format(filename, year, qt), index=False, encoding='utf-8-sig')
            #     print('{}: {}'.format(filename, df.shape[0]))
            # try:
            #     if not '수익률' in list(df.columns):
            #         df['수익률'] = ((df['최신가격'] - df['기준가격']) / df['기준가격']) * 100
            #     print('수익률: {}'.format(df['수익률'].mean()))
            #     #return df['수익률'].mean()
            #     pass
        return 
    
    def insertResultData(self, df, tablename):
        print('insertResultData({})==============================================================================================='.format(tablename))
        print('{}테이블을 DB에 저장합니다.'.format(tablename))
        cols = df.columns
        dtypes = ['VARCHAR(20)', 'VARCHAR(30)'] + ['FLOAT']*(df.shape[1]-2)
        empty_check = self.mariaShowData(tablename)
        if empty_check is None:
            self.mariaCreateTable(tablename, cols, dtypes)
        else:
            self.mariaSql('drop table ' + tablename)
            self.mariaCreateTable(tablename, cols, dtypes)
        for i in range(df.shape[0]):
            self.mariaInsertData(tablename, tuple(df.iloc[i]))
            self.mariaCommitDB()
        return
    
    def showPlot(self, df):
        x = np.arange(df.shape[1])
        xticks = list(df.columns)
        values = df.iloc[0]

        plt.bar(x, values)
        plt.xticks(x, xticks)
        plt.show()
        return

    def sliceDf(self, df, colname, divide):
        df = df.sort_values(colname).reset_index(drop=True)
        df_list = []
        for i in range(divide):
            df_list.append(df[int(df.shape[0] / divide)*i:int(df.shape[0] / divide)*(i+1)])
        return df_list
    
    def mergeAll(self, wish_list, year, qt):
        print('mergeAll({}, {})=============================================================================================='.format(year, qt))
        print('{} 데이터를 모두 병합합니다.'.format(wish_list))
        if qt != '12':
            cols = [str(y)+'_'+qt+'q' for y in list(range(year, year-4, -1))]
        else:
            cols = [str(y)+'_12' for y in list(range(year, year-4, -1))]
        data = {}
        for w in wish_list:
            if qt != '12':
                tablename = f'fnguideQ_{w}_{year}_{qt}'
                df = self.mariaShowData(tablename).replace(-123456, np.nan)[['code', 'name']+cols]
                df.columns = ['code', 'name', 'year_q', 'bfyear_q', 'bbfyear_q', 'bbbfyear_q']
            else:
                tablename = f'fnguide_{w}_{year}_{qt}'
                df = self.mariaShowData(tablename).replace(-123456, np.nan)[['code', 'name']+cols]
                df.columns = ['code', 'name', 'year', 'bfyear', 'bbfyear', 'bbbfyear']
            modified_cols = list(df.columns)[2:]
            # 빠져선 안될 값들이 빠졌을 경우 데이터 버리기
            if w in ['botongju', 'jabongeum', 'jasanchonggye', 'yeongeobiig', 'yudongbuchae', 'yudongjasan', 'danggisuniig', 'jibaejujujibun', 'jibaejujusuniig']:
                df = df.dropna().reset_index(drop=True)
            else:
                df = df.fillna(0).reset_index(drop=True)
            df.columns = ['code', 'name'] + [w+'_'+c for c in modified_cols]
            data[w] = df

        df = data[list(data.keys())[0]]
        for k in list(data.keys())[1:]:
            bf_amount = df.shape[0]
            if k in ['botongju', 'jabongeum', 'jasanchonggye', 'yeongeobiig', 'yudongbuchae', 'yudongjasan', 'danggisuniig', 'jibaejujujibun', 'jibaejujusuniig']:
                df = pd.merge(df, data[k], on=['code', 'name'])
            else:
                df = pd.merge(df, data[k], on=['code', 'name'], how='outer').fillna(0)
            print(k + '의 null 값으로 인해 데이터가 줄었습니다.')
            print('{} -> {}'.format(bf_amount, df.shape[0]))
        return df

    def mergeByYear(self, wish_list, year):
        print('mergeByYear({})=============================================================================================='.format(year))
        print('{} 데이터를 모두 병합합니다.'.format(wish_list))
        w = wish_list[0]
        df = self.mariaShowData('data_' + w).replace(-123456, np.nan)[['code', 'name', '{}_12'.format(year)]]
        if w in ['botongju', 'jabongeum', 'jasanchonggye', 'yeongeobiig', 'yudongbuchae', 'yudongjasan', 'danggisuniig', 'jibaejujujibun', 'jibaejujusuniig']:
            df = df.dropna().reset_index(drop=True)
        else:
            df = df.fillna(0).reset_index(drop=True)
        df.columns = ['code', 'name', w]

        for w in wish_list[1:]:
            tmp = self.mariaShowData('data_' + w).replace(-123456, np.nan)[['code', 'name', '{}_12'.format(year)]]
            if w in ['botongju', 'jabongeum', 'jasanchonggye', 'yeongeobiig', 'yudongbuchae', 'yudongjasan', 'danggisuniig', 'jibaejujujibun', 'jibaejujusuniig']:
                tmp = tmp.dropna().reset_index(drop=True)
            else:
                tmp = tmp.fillna(0).reset_index(drop=True)
            tmp.columns = ['code', 'name', w]
            bf_amount = df.shape[0]
            df = pd.merge(df, tmp, on=['code', 'name'])
            print(w + '의 null 값으로 인해 데이터가 줄었습니다.')
            print('{} -> {}'.format(bf_amount, df.shape[0]))
        return df

    def calBps(self, df):
        print('calBps()==============================================================================================')
        print('BPS 값을 계산하여 테이블에 추가합니다.')
        df['bps'] = (df['jabonchonggye'] / df['botongju']) * 1000
        return df

    def calRoa(self, df, year):
        print('calRoa({})=============================================================================================='.format(year))
        print('ROA 값을 계산하여 테이블에 추가합니다.')
        tmp = self.mariaShowData('data_jasanchonggye').rename(columns={'{}_12'.format(year-1):'jasanchonggye_bfyear'}).replace(-123456, np.nan)[['code', 'name', 'jasanchonggye_bfyear']]
        df = pd.merge(df, tmp, on=['code', 'name'])
        df['roa'] = df['danggisuniig'] / ((df['jasanchonggye_bfyear'] + df['jasanchonggye_bfyear'])/2)
        return df.reset_index(drop=True)
    
    def calPbr(self, df, year, std_date):
        print('calPbr({})=============================================================================================='.format(year))
        print('PBR 값을 계산하여 테이블에 추가합니다.')
        df = self.mergePrice(df, std_date=std_date, price_name='std_price', get_size=False)
        df = self.calBps(df)
        df['pbr'] = df['std_price'] / df['bps']
        return df.reset_index(drop=True)

    def getConditionTable(self, gb, subject, periods, method, process_na):
        tablename = 'fnguide{}_{}_2020_12'.format(gb, subject)
        if process_na == 'drop':
            curr_df = self.mariaShowData(tablename).replace(-123456, np.nan).dropna().reset_index(drop=True)
        else:
            curr_df = self.mariaShowData(tablename).replace(-123456, np.nan).fillna(process_na).reset_index(drop=True)

        new_df = curr_df[['code', 'name']]
        if method == 'sum':
            for c in range(periods, curr_df.shape[1]-1):
                new_df['{}_{}'.format(subject, curr_df.columns[-c])] = [0] * new_df.shape[0]
                for i in range(periods):
                    new_df['{}_{}'.format(subject, curr_df.columns[-c])] += curr_df[curr_df.columns[-c+1]]

        elif method == 'average':
            for c in range(periods, curr_df.shape[1]-1):
                new_df['{}_{}'.format(subject, curr_df.columns[-c])] = [0] * new_df.shape[0]
                for i in range(c):
                    new_df['{}_{}'.format(subject, curr_df.columns[-c])] += curr_df[curr_df.columns[-c+1]]
                new_df['{}_{}'.format(subject, curr_df.columns[-c])] /= periods
                
        return new_df

    def getMergedConditionTable(self, gb, subjects, periods, methods, process_na_list):
        data = {}
        for i in range(len(subjects)):
            data[subjects[i]] = self.getConditionTable(gb=gb, subject=subjects[i], periods=periods[i], method=methods[i], process_na=process_na_list[i])
    
        df = data[subjects[0]]
        print('종목 수: {}'.format(df.shape[0]))
        for s in subjects[1:]:
            bf_size = df.shape[0]
            df = pd.merge(df, data[s], on=['code', 'name']).reset_index(drop=True)
            print('{}의 null 값으로 인해 종목 수가 줄었습니다. {} -> {}'.format(s, bf_size, df.shape[0]))
        return list(data[subjects[0]].columns), df

    def calFormula(self, df, new_subject, formula, std_cols, subjects):
        for s in subjects:
            formula = formula.replace(s, "df['{}_']".format(s, s))
            formula = formula.replace("_']", "_{}'.format(curr_now)]")
        
        for i in range(2, len(std_cols)):
            curr_now = std_cols[i].split('_')[-2] + '_' + std_cols[i].split('_')[-1]
            df['{}_{}'.format(new_subject, curr_now)] = eval(formula)
        return df

    def getNewSubjectDf(self, new_subject):
        if new_subject == '트레일링per_종가':
            # 최근 4분기 당기순이익 합 / 최근 4분기 총주식수 평균
            gb = 'q'
            subjects = ['danggisuniig', 'botongju', 'useonju']
            periods = [4, 4, 4]
            methods = ['sum', 'average', 'average']
            process_na_list = ['drop', 'drop', 0]
            std_cols, df = self.getMergedConditionTable(gb=gb, subjects=subjects, periods=periods, methods=methods, process_na_list=process_na_list)

            formula = '(danggisuniig / (botongju + useonju))'
            df = self.calFormula(df, new_subject, formula, std_cols, subjects)
            return df
        elif new_subject == 'NCAV':
            gb = ['yudongjasan', 'buchaechonggye', 'sigachongaeg']
            periods = [1, 1, 1]
            methods = [None, None, None]
            process_na_list = ['drop', 'drop', 'drop']
            std_cols, df = self.getMergedConditionTable(gb=gb, subjects=subjects, periods=periods, methods=methods, process_na_list=process_na_list)

        else:
            print('등록되지 않은 과목입니다.')
        
    # 수동으로 그냥 수집
    def insertDartDate(self):
        import OpenDartReader
        all_df = []
        # ==== 0. 객체 생성 ====
        # 객체 생성 (API KEY 지정) 
        api_key = 'd385c1c86c664c51c2a3b2579494b800897d8f62 '

        dart = OpenDartReader(api_key)

        code_list = list(self.getStockNameCodeDf()['종목코드'])
        print(code_list)
        done_list = list(self.mariaShowData('data_dartdate')['code'])

        periods = ['2015_12']
        for y in ['2016', '2017', '2018', '2019', '2020']:
            for q in ['_03', '_06', '_09', '_12']:
                periods.append(y+q)

        for c in code_list:
            time.sleep(0.5)
            print(c)
            #try:
            if c in done_list:
                continue
            data_list = dart.list(c, start='2016', final=False) 

            new_data_list = data_list[data_list['report_nm'].str.startswith('반기보고서') | data_list['report_nm'].str.startswith('사업보고서') | data_list['report_nm'].str.startswith('분기보고서')].reset_index(drop=True)
            report_nm_list = list(new_data_list['report_nm'])
            report_nm_list = [r.replace('사업보고서', '').replace('제출기한연장신고서', '').replace('분기보고서', '').replace('반기보고서', '').replace('(', '').replace(')', '').replace('.', '_').replace(' ', '') for r in report_nm_list]
            new_data_list['report_nm'] = report_nm_list
            new_data_list = new_data_list[['report_nm', 'rcept_dt']]
            new_data_list[new_data_list['report_nm'] >= '2015_12']
            table_data = [c]

            for p in periods:
                try:
                    table_data.append(new_data_list[new_data_list['report_nm'] == p]['rcept_dt'].iloc[0])
                except:
                    table_data.append('no date')

            self.mariaInsertData('data_dartdate', tuple(table_data))
            self.mariaCommitDB()
            #except:
            #    pass

    def getBuyDay(self, date_df, code, period):
        return date_df[date_df['code'] == code][period].iloc[0]
    
    def computeValue(self, compute_type, periods, buy_days, sell_days):
        '''
        최종 compute하고 종목을 뽑아내는 함수
        
        param compute_type: value라면 주당가치분석, dcf라면 dcf분석
        '''

        if compute_type == 'value':
            an_wish_list = ['jabongeum', 'jabonchonggye', 'jasanchonggye', 'buchaechonggye', 'muhyeongjasan',
                            'yudongjasan', 'yudongbuchae',
                            'hyeongeumseongjasan', 'maechulaeg', 'yeongeobiig', 'janggichaibgeum', 'dangichaibgeum',
                            'danggisuniig',
                            'jibaejujujibun', 'botongju', 'useonju', 'jibaejujusuniig', 'iyeonbeobinsejasan',
                            'iyeonbeobinsebuchae', 'baedanggeum']
            qt_wish_list = ['danggisuniig', 'jibaejujusuniig', 'jibaejujujibun']

        elif compute_type == 'dcf':
            an_wish_list = ['jibaejujujibun', 'botongju', 'useonju', 'jibaejujusuniig', 'danggisuniig', 'yeongeobiig',
                           'jabonchonggye', 'maechulaeg', 'jasanchonggye', 'yudongbuchae', 'yudongjasan', 'buchaechonggye']
            qt_wish_list = ['danggisuniig', 'jibaejujusuniig', 'jibaejujujibun']
        # ecxception1
        else:
            print('잘못된 compute_type입니다.')
            return


        years = [p.split('_')[0] for p in periods]
        qts = [p.split('_')[1] for p in periods]

        if len(periods) == len(sell_days):
            pass
        # exception2
        else:
            print('Hyper Parameter의 수가 맞지 않습니다.')
            return
        res_data = {}

        for i in range(len(periods)):
            if qts[i] != '12':
                df_qt = self.mergeAll(wish_list=qt_wish_list, year=int(years[i]), qt=qts[i])
                df_an = self.mergeAll(wish_list=an_wish_list, year=int(years[i]) - 1, qt='12')
                df = pd.merge(df_qt, df_an, on=['code', 'name']).reset_index(drop=True)
                df = self.calValueDanggisuniig(df)
                if compute_type == 'dcf':
                    df = self.mergeDcfData(df=df, year=int(years[i])-1)
            else:
                df = self.mergeAll(wish_list=an_wish_list, year=int(years[i]), qt='12')
                if compute_type == 'dcf':
                    df = self.mergeDcfData(df=df, year=int(years[i]))
            df = self.mergeJasaju(df=df)
            df = self.calValueRoe(df=df, year=int(years[i]), qt=qts[i])
            df = self.calValueEps(df=df)
            df = self.mergeYogusuiglyul(df=df)
            if compute_type == 'dcf':
                df = self.calValueFcf(df=df)
            res_data[periods[i]] = self.mergePrice(df=df, std_date=buy_days[i], price_name='buy_price', get_size=False)
            res_data[periods[i]] = self.computeDfValueEvaluation(res_data[periods[i]], method=compute_type)
            res_data[periods[i]] = self.calScores(df=res_data[periods[i]])
            tablename = 'data_{}_result_{}'.format(compute_type, periods[i])
            self.insertResultData(df=res_data[periods[i]], tablename=tablename)

        for i in range(len(periods)-1, len(periods)):
            '''
            a. 기준가격 > 주당가치분석
            b. 주당가치분석 < 기업가치
            c. 영업이익 3년 연속 상승
            d. 부채비율 n% 이하
            e. 본질가치 > 주당기업가치
            f. 현재가 < 적정주가_dcf
            g. 현재가 추가하기
            h. 현재가 < 적정주가_rim
            i. roe > 요구수익률
            orgin: 원래 데이터
            '''
            if compute_type == 'dcf':
                data = {}
                res_tablename = 'data_dcf_result_{}'.format(periods[i])
                c_df = self.analyzeData('c', res_tablename, std_date=buy_days[i], today=sell_days[i])
                d_df = self.analyzeData('d', res_tablename, std_date=buy_days[i], today=sell_days[i], dept_rate1=0, dept_rate2=150)
                f_df = self.analyzeData('f', res_tablename, std_date=buy_days[i], today=sell_days[i])
                h_df = self.analyzeData('h', res_tablename, std_date=buy_days[i], today=sell_days[i])
                orgin_df = self.analyzeData('orgin', res_tablename, std_date=buy_days[i] , today=sell_days[i])

                df_list = [c_df, d_df, f_df, h_df]
                filter_list = ['영업이익', '부채비율_100_250', 'dcf', 'rim']
                file_name = filter_list[0]
                for f in filter_list[1:]:
                    file_name += '_' + f
                self.mergeAnalyzeTable(data=data, df_list=df_list, filter_list=filter_list, year=years[i], qt=qts[i], save_excel=True)
            elif compute_type == 'value':
                data = {}
                res_tablename = 'data_value_result_{}'.format(periods[i])
                # a_df = self.analyzeData('a', res_tablename, std_date=buy_days[i], today=sell_days[i])
                c_df = self.analyzeData('c', res_tablename, std_date=buy_days[i], today=sell_days[i])
                d_df = self.analyzeData('d', res_tablename, std_date=buy_days[i], today=sell_days[i], dept_rate1=0,
                                         dept_rate2=150)
                e_df = self.analyzeData('e', res_tablename, std_date=buy_days[i], today=sell_days[i])
                h_df = self.analyzeData('h', res_tablename, std_date=buy_days[i], today=sell_days[i])
                orgin_df = self.analyzeData('orgin', res_tablename, std_date=buy_days[i], today=sell_days[i])

                # df_list = [a_df, c_df, d_df, e_df, h_df]
                df_list = [c_df, d_df, e_df, h_df]
                # filter_list = ['기준가격', '영업이익', '부채비율_0_150', '본질', 'rim']
                filter_list = ['영업이익', '부채비율_0_150', '본질', 'rim']
                file_name = filter_list[0]
                for f in filter_list[1:]:
                    file_name += '_' + f
                self.mergeAnalyzeTable(data=data, df_list=df_list, filter_list=filter_list, year=years[i], qt=qts[i],
                                       save_excel=True)



            score_tablename = 'data_{}_score_{}'.format(compute_type, periods[i])

            # mo_std_date = buy_days[i][:4] + '-' + buy_days[i][4:6] + '-' + buy_days[i][6:]
            # mo_today = sell_days[i][:4] + '-' + sell_days[i][4:6] + '-' + sell_days[i][6:]

            #     data_list = [file_name1, file_name2, file_name3, file_name4]
            #     name_list = ['dcf_100_250', 'dcf_0_150', 'value_100_250', 'value_0_150']

            #     average_list = []
            #     for name, file_name in enumerate(data_list):
            #         if name > 1:
            #             df = pd.merge(data[file_name][['code']], orgin_df2, on=['code'])
            #         else:
            #             df = pd.merge(data[file_name][['code']], orgin_df1, on=['code'])
            #         scaler = MinMaxScaler()
            #         average_price = []
            #         if df.shape[0] == 0:
            #             pass
            #         else:
            #             for j in range(df.shape[0]):
            #                 curr_stock = df['code'].iloc[j]
            #                 curr_table = fdr.DataReader(curr_stock, mo_std_date, sell_day).reset_index(drop=False)
            #                 plt.figure(figsize=(12, 4))
            #                 plt.plot(curr_table['Close'], color='black', label='Close')
            #                 plt.xticks(np.arange(0, curr_table.shape[0], step=100), [str(curr_table['Date'].iloc[x]).split(' ')[0] for x in np.arange(0, curr_table.shape[0], step=100)],
            #                           rotation=30)
            #                 plt.title(curr_stock)

            #                 plt.axhline(curr_table['Close'].iloc[0] * 1.2, 0, 1, color='red', linestyle='-', linewidth='1', label='profit')
            #                 plt.axhline(curr_table['Close'].iloc[0] * 1.1, 0, 1, color='red', linestyle='--', linewidth='1', label='preserve')
            #                 plt.axhline(curr_table['Close'].iloc[0] * 0.8, 0, 1, color='blue', linestyle='-', linewidth='1', label='loss')
            #                 plt.legend()

            #                 if not file_name in  os.listdir():
            #                     os.mkdir(file_name)
            #                 plt.savefig(os.path.join(file_name, '{}_{}.png'.format(periods[i], curr_stock)))

            #                 curr_close = pd.DataFrame(scaler.fit_transform(curr_table[['Close']]))
            #                 average_price.append(list(curr_close.transpose().iloc[0]))

            #             average_price = pd.DataFrame(average_price)
            #             average_list.append([[average_price[c].mean() for c in average_price.columns] , name_list[name]])
            #     plt.figure(figsize=(12, 4))
            #     for a in range(len(average_list)):
            #         plt.plot(average_list[a][0], label=average_list[a][1])
            #     plt.xticks(np.arange(0, curr_table.shape[0], step=100), [str(curr_table['Date'].iloc[x]).split(' ')[0] for x in np.arange(0, curr_table.shape[0], step=100)],
            #           rotation=30)
            #     plt.title(periods[i])
            #     plt.legend()
            #     plt.savefig('{}.png'.format(periods[i]))

            df = pd.merge(data[file_name][['code']], orgin_df, on=['code'])
            cols = df.columns
            dtypes = ['VARCHAR(20)', 'VARCHAR(30)'] + ['FLOAT'] * (df.shape[1] - 2)
            empty_check = self.mariaShowData(score_tablename)
            if empty_check is None:
                self.mariaCreateTable(score_tablename, cols, dtypes)
            else:
                self.mariaSql('drop table ' + score_tablename)
                self.mariaCreateTable(score_tablename, cols, dtypes)
            for i in range(df.shape[0]):
                self.mariaInsertData(score_tablename, tuple(df.iloc[i]))
                self.mariaCommitDB()


