
import datetime
import time
import sys
import requests
from bs4 import BeautifulSoup
from cjw_mysql import *
from utils import *



global giza_list
global keyword_list
giza_list = ['박정수', '김민기', '유현석', '권효중', '설경진', '박형수', '최두선', '유준하', '이완기', '배요한', '이민우']
keyword_list = ['독점', '판권', 'FDA', '국내최초', '세계최초', '도네페질 패치',
                '치매', '임상 3상', '긴급', '구글', '아마존', '애플', '빌게이츠', '빌 게이츠', '나스닥',
                '마이크로소프트', '임박', '문대통령', '대통령', '합병', '게임체인저', '게임 체인저', '변이',
                '효능확인', '효능 확인', 'WHO', '국산', '핵심', '유일', '실질적', '연합', '동맹',
                '췌장암', '조현병', '알츠하이머', '급박', '급증', '독과점', '시장 진출', '시장진출', '넷플릭스', '페이스북', 
                '트위터', '구글', '인수합병', 'M&A', '인수', '승인', 'EU', '방한']

def checkKeyword(curr_news_title, curr_datetime, curr_news_datetime, curr_news_url, main):
    for giza in giza_list:
        if giza in curr_news_title:
            for keyword in keyword_list:
                if keyword in curr_news_title:
                    if '[특징주]' in curr_news_title:
                        main.mariaInsertData('data_good_news',
                                             tuple([curr_news_title, curr_datetime, curr_news_datetime,
                                                    curr_news_url]))
                    main.mariaInsertData('data_bad_news',
                                         tuple([curr_news_title, curr_datetime, curr_news_datetime,
                                                curr_news_url]))
                    main.mariaCommitDB()
                    return


def collect(gb):
    curr_url = None
    curr_titles_selector = None
    curr_urls_selector = None
    curr_content_selector = None
    curr_time_selector = None
    getTime = None

    if gb == 'naver':
        curr_url = 'https://finance.naver.com/news/news_list.nhn?mode=LSS2D&section_id=101&section_id2=258'
        curr_titles_selector = '.articleSubject > a'
        curr_urls_selector = '.articleSubject > a'
        curr_content_selector = '#content'
        curr_time_selector = 'span.article_date'
        getTime = lambda time: time + ':00'

    elif gb == 'yna':
        curr_url = 'https://www.yna.co.kr/economy/all?site=navi_economy_depth02'
        curr_titles_selector = '#container > div > div > div.section01 > section > div.list-type038 > ul > li > div > div.news-con > a > strong'
        curr_urls_selector = '#container > div > div > div.section01 > section > div.list-type038 > ul > li > div > div.news-con > a'
        curr_content_selector = '#articleWrap > div.content01.scroll-article-zone01 > div > div > article'
        curr_time_selector = '#articleWrap > div.content03 > header > p'
        getTime = lambda time: time.replace('송고시간', '') + ':00'

    elif gb == 'financial':
        curr_url = 'https://www.fnnews.com/section/002001000'
        curr_titles_selector = '#secNo1 > li > a > strong'
        curr_urls_selector = '#secNo1 > li > a:nth-child(1)'
        curr_content_selector = '#article_content'
        curr_time_selector = '#root > div.view_hd > div > div.byline'
        getTime = lambda time: time.replace('\n', '').replace('파이낸셜뉴스입력 ', '').split('수정')[0] + ':00'

    elif gb == 'seoul':
        curr_url = 'https://www.sedaily.com/NewsList/GA05'
        curr_titles_selector = '#ContentForm > div > ul > li > a > div > h3'
        curr_urls_selector = '#ContentForm > div > ul > li > a'
        curr_content_selector = 'div.article_info'
        curr_time_selector = 'div.article_info > span:nth-of-type(1)'
        getTime = lambda time: time.replace('입력', '').replace('.', '-').replace('\n', '')

    elif gb == 'edaily':
        curr_url = 'https://www.edaily.co.kr/articles/stock/stock'
        curr_titles_selector = '#newsList > div > a > ul > li:nth-child(1)'
        curr_urls_selector = '#newsList > div > a'
        curr_content_selector = '#contents > section.center1080.position_r > section.aside_left > div.article_news > div.newscontainer > div.news_body'
        curr_time_selector = '#contents > section.center1080.position_r > section.aside_left > div.article_news > div.news_titles > div > div > ul > li:nth-of-type(1) > p:nth-of-type(2)'
        def getTime(time):
            if '오후' in time:
                time = time.replace('오후 ', '').replace('수정 ', '')
                curr_hour = int(time.split(' ')[1].split(':')[0])
                curr_hour += 12
                curr_hour = str(curr_hour)
                curr_date = time.split(' ')[0]
                curr_min = time.split(' ')[1].split(':')[1]
                curr_sec = time.split(' ')[1].split(':')[2]
                time = f'{curr_date} {curr_hour}:{curr_min}:{curr_sec}'
            else:
                time = time.replace('오전 ', '').replace('수정 ', '').replace(' 12', ' 00')
            return time

    elif gb == 'etoday':
        curr_url = 'https://www.etoday.co.kr/news/section/subsection?MID=1202'
        curr_titles_selector = '#list_W > li > a > div > div.cluster_text_headline21.t_reduce'
        curr_urls_selector = '#list_W > li > a'
        curr_content_selector = 'body > div.wrap > article.containerWrap > section.news_dtail_view_top_wrap > div > div > div > ul > li > dl > dd'
        curr_time_selector = 'body > div.wrap > article.containerWrap > section.news_dtail_view_top_wrap > div > div > span'
        getTime = lambda time: time.split(' ')[1] + ' ' + time.split(' ')[2] + ':00'

    elif gb == 'asia':
        curr_url = 'https://www.asiae.co.kr/list/feature'
        curr_titles_selector = '#container > div.content > div.cont_listarea > div.cont_list > div > h3 > a'
        curr_urls_selector = '#container > div.content > div.cont_listarea > div.cont_list > div > h3 > a'
        curr_content_selector = '#txt_area'
        curr_time_selector = '#container > div.content > div.cont_sub > div.area_title > div > p.user_data'
        getTime = lambda time: (time.split(' ')[1].replace('.', '-') + ' ' + time.split(' ')[2]).split('\t')[0] + ':00'

    maria = Maria()
    host = '3.37.26.5'
    user = 'root'
    password = 'sa1234'
    db = 'news'
    port = 1889
    maria.setMaria(host=host, user=user, password=password, db=db, port=port)
    title_list = maria.mariaShowData('data_news_real')['title'].tolist()


    while True:
        driver = setChrome(visualize=False)
        driver.get(curr_url)
        curr_titles = driver.find_elements_by_css_selector(curr_titles_selector)
        curr_urls = driver.find_elements_by_css_selector(curr_urls_selector)
        try:
            if len(curr_titles) > 1:
                for i in range(len(curr_titles)):
                    try:
                        curr_news_url = curr_urls[i].get_attribute('href')
                    except:
                        break
                    req = requests.get(curr_news_url)
                    if not req.content:
                        continue
                    html = req.text
                    soup = BeautifulSoup(html, 'html.parser')
                    curr_content = soup.select_one(curr_content_selector).text
                    curr_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    curr_time = getTime(soup.select_one(curr_time_selector).text)
                    curr_news_title = None
                    for giza in giza_list:
                        if giza in curr_content:
                            curr_news_title = '[{} 기자][{}]{}'.format(giza, gb, curr_titles[i].text)
                            break
                        else:
                            curr_news_title = '[{}]{}'.format(gb, curr_titles[i].text)
                    if curr_news_title in title_list:
                        continue
                    checkKeyword(curr_news_title, curr_datetime, curr_time, curr_news_url, maria)
                    table_data = [curr_news_title, curr_datetime, curr_time, curr_news_url]
                    maria.mariaInsertData('data_news_real', tuple(table_data))
                    print('Title[{}] DateTime[{}] LocalTime[{}]'.format(curr_news_title, curr_time,
                                                                        curr_datetime))
                    title_list.append(curr_news_title)
                    maria.mariaCommitDB()
            driver.close()
            driver.quit()
            time.sleep(5)
            if gb == 'asia':
                time.sleep(60)
        except Exception as e:
            print(e)
            driver.close()
            driver.quit()
            pass


def testCollector(title, content):
    new_title = None
    for giza in giza_list:
        if giza in content:
            new_title = '[{} 기자][Test]{}'.format(giza, title)
            break
        else:
            new_title = '[Test]{}'.format(title)
    maria = Maria()
    curr_datetime = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    checkKeyword(new_title, curr_datetime, curr_datetime, 'www.naver.com', maria)

gb = sys.argv[1]
print(gb)
collect(gb)
