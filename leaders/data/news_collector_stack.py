from selenium import webdriver
import datetime
from tqdm import tqdm
import chromedriver_autoinstaller
import requests
from bs4 import BeautifulSoup
from cjw_mysql import *

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

driver = setChrome(False)
today_date = str(datetime.datetime.now()).split(' ')[0].replace('-', '')

main = Maria()
main.setMaria(host='3.37.26.5', user='root', password='sa1234', db='news', charset='utf8', port=1889)

table_list = main.mariaShowTables()

cnt = 0
while True:
    try:
        for i in tqdm(range(cnt, 1000)):
            curr_date = str(datetime.datetime.strptime(today_date, '%Y%m%d') - datetime.timedelta(days=i)).split(' ')[0].replace('-', '')
            curr_ymonth = curr_date[:6]
            tablename = f'data_news_{curr_ymonth}'
            is_first = True
            if not tablename in table_list:
                cols = ['title', 'date', 'time', 'content']
                dtypes = ['varchar(200)', 'varchar(20)', 'varchar(20)', 'TEXT']
                main.mariaCreateTable(tablename, cols, dtypes)
                table_list.append(tablename)
                title_list = []
                is_first = False
            else:
                if is_first:
                    title_list = main.mariaShowData(tablename)['title'].tolist()
                    is_first = False
            curr_page = 1
            while True:
                curr_url = 'https://finance.naver.com/news/news_list.nhn?mode=LSS2D&section_id=101&section_id2=258&date={}&page={}'.format(curr_date, curr_page)
                driver.get(curr_url)
                day_finish = False
                curr_elements = driver.find_elements_by_css_selector('.articleSubject > a')
                for i in range(20):
                    try:
                        curr_element = curr_elements[i]
                    except:
                        day_finish=True
                        break
                    curr_news_datetime = driver.find_elements_by_css_selector('.wdate')[i].text
                    curr_news_title = curr_element.text
                    if not curr_news_title in title_list:
                        curr_news_url = curr_element.get_attribute('href')
                        try:
                            req = requests.get(curr_news_url)
                            if not req.content:
                                continue
                            html = req.text
                            soup = BeautifulSoup(html, 'html.parser')
                            curr_content = soup.select_one('#content').text
                            curr_news_content = curr_content.split('재배포')[0]
                        except:
                            pass
                        table_data = [curr_news_title, curr_news_datetime.split(' ')[0], curr_news_datetime.split(' ')[1], curr_news_content]
                        main.mariaInsertData(tablename, tuple(table_data))
                        main.mariaCommitDB()
                        title_list.append(curr_news_title)
                curr_page += 1
                if day_finish:
                    day_finish = False
                    break
    except:
        cnt = i
        try:
            driver.close()
            driver.quit()
        except:
            pass
        pass
