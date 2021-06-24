from fnguide import *
from tqdm import tqdm


fnguide = Data()
fnguide.setMaria()
curr_month = '2021_05'
tablename = f'fnguide_stocks_{curr_month}'
fnguide_stocks_df = fnguide.mariaShowData(tablename)
updated_code_list = fnguide_stocks_df['code'].tolist()

stock_list = fnguide.getStockNameCodeDf()['종목코드'].tolist()
stocks_css_selector = '#svdMainGrid1 > table > tbody > tr:nth-child(7) > td:nth-child(2)'
jasaju_css_selector = '#dataTable > tbody > tr:nth-child(5) > td:nth-child(3)'

driver = fnguide.setChrome(False)
for curr_code in tqdm(stock_list):
    if curr_code in updated_code_list:
        continue
    try:
        curr_url = f'https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{curr_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701'
        driver.get(curr_url)
        curr_stocks = driver.find_element_by_css_selector(stocks_css_selector).text
        curr_stocks = curr_stocks.split('/')
        curr_btju = int(curr_stocks[0].replace(',', ''))
        curr_usju = int(curr_stocks[1].replace(',', ''))
        curr_url = f'https://comp.fnguide.com/SVO2/ASP/SVD_shareanalysis.asp?pGB=1&gicode=A{curr_code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=109&stkGb=701'
        driver.get(curr_url)
        curr_jasaju = driver.find_element_by_css_selector(jasaju_css_selector).text
        if curr_jasaju == ' ':
            curr_jasaju = 0
        else:
            curr_jasaju = int(curr_jasaju.replace(',', ''))
        table_data = [curr_code, curr_btju, curr_usju, curr_jasaju]
        fnguide.mariaInsertData(tablename, tuple(table_data))
        fnguide.mariaCommitDB()
        updated_code_list.append(curr_code)
    except:
        print(curr_code)
        pass

