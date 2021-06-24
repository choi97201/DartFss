from selenium import webdriver
import chromedriver_autoinstaller
import pandas as pd

def setChrome(visualize):
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

def get_market_df(latest=True):
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
    if latest:
        return orgin_df

    market_df = orgin_df.rename(columns={'종목코드': 'Code', '회사명': 'Name'})
    market_df2 = pd.read_excel('../codes.xlsx').rename(
        columns={'code': 'Code', 'name': 'Name', 'market': 'Market'})
    market_df2['Market'] = market_df2['Market'].replace('유가', 'kospi').replace('코스닥', 'kosdaq')
    market_df = pd.concat([market_df, market_df2]).drop_duplicates('Code', keep='first').reset_index(drop=True)
    return market_df