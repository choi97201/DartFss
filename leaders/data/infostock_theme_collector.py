from utils import *
import numpy as np
import time
from tqdm import tqdm

url = 'http://m.infostock.co.kr/sector/sector.asp?mode=w#'

driver = setChrome(True)

driver.get(url)

theme_list1 = pd.read_html(driver.page_source)[1][0].tolist()
theme_list1 = [theme.replace('◎ ', '') for theme in theme_list1 if type(theme) == str]

theme_list2 = pd.read_html(driver.page_source)[1][2].tolist()
theme_list2 = [theme.replace('◎ ', '') for theme in theme_list2 if type(theme) == str]

theme_df = []

for theme_idx in tqdm(range(len(theme_list1))):
    curr_theme = theme_list1[theme_idx]

    curr_df = pd.DataFrame()

    btn_css = f'#data > table > tbody > tr:nth-child({theme_idx + 1}) > td:nth-child(1) > a'

    btn = driver.find_element_by_css_selector(btn_css)

    btn.click()

    curr_list = pd.read_html(driver.page_source)[1].iloc[4:][0].tolist()
    curr_list = [curr_code.replace("'", '').replace('-', '').replace('(', '').replace(')', '') for curr_code in
                 curr_list]

    code_list = [curr_code.split(' ')[2] for curr_code in curr_list]
    name_list = [curr_code.split(' ')[1] for curr_code in curr_list]

    curr_df['code'] = code_list
    curr_df['name'] = name_list
    curr_df['theme'] = [curr_theme] * curr_df.shape[0]
    theme_df.append(curr_df)

    driver.back()
    time.sleep(0.1)

driver.refresh()
time.sleep(1)

for theme_idx in tqdm(range(len(theme_list2))):
    curr_theme = theme_list2[theme_idx]

    curr_df = pd.DataFrame()

    btn_css = f'#data > table > tbody > tr:nth-child({theme_idx + 1}) > td:nth-child(3) > a'

    btn = driver.find_element_by_css_selector(btn_css)

    btn.click()

    curr_list = pd.read_html(driver.page_source)[1].iloc[4:][0].tolist()
    curr_list = [curr_code.replace("'", '').replace('-', '').replace('(', '').replace(')', '') for curr_code in
                 curr_list]

    code_list = [curr_code.split(' ')[2] for curr_code in curr_list]
    name_list = [curr_code.split(' ')[1] for curr_code in curr_list]

    curr_df['code'] = code_list
    curr_df['name'] = name_list
    curr_df['theme'] = [curr_theme] * curr_df.shape[0]
    theme_df.append(curr_df)

    driver.back()
    time.sleep(0.1)

for theme_idx in tqdm(range(len(theme_list2))):
    curr_theme = theme_list2[theme_idx]

    curr_df = pd.DataFrame()

    btn_css = f'#data > table > tbody > tr:nth-child({theme_idx + 1}) > td:nth-child(3) > a'

    btn = driver.find_element_by_css_selector(btn_css)

    btn.click()

    curr_list = pd.read_html(driver.page_source)[1].iloc[4:][0].tolist()
    curr_list = [curr_code.replace("'", '').replace('-', '').replace('(', '').replace(')', '') for curr_code in
                 curr_list]

    code_list = [curr_code.split(' ')[2] for curr_code in curr_list]
    name_list = [curr_code.split(' ')[1] for curr_code in curr_list]

    curr_df['code'] = code_list
    curr_df['name'] = name_list
    curr_df['theme'] = [curr_theme] * curr_df.shape[0]
    theme_df.append(curr_df)

    driver.back()
    time.sleep(0.1)

theme_df = pd.concat(theme_df).reset_index(drop=True)

error_list = []

for i in range(theme_df.shape[0]):
    try:
        int(theme_df['code'][i])
    except:
        error_list.append(theme_df['code'][i])

for error in error_list:
    theme_df['code'] = theme_df['code'].replace(error, np.nan)

theme_df = theme_df.dropna().reset_index(drop=True)

code_dict = {}
res = []

for idx in range(theme_df.shape[0]):
    curr_code = theme_df['code'][idx]
    curr_name = theme_df['name'][idx]
    code_dict[curr_code] = [curr_name]

for idx in range(theme_df.shape[0]):
    curr_code = theme_df['code'][idx]
    curr_theme = theme_df['theme'][idx]
    code_dict[curr_code].append(curr_theme)

for idx in range(len(list(code_dict.keys()))):
    curr_code = list(code_dict.keys())[idx]
    curr_data = list(code_dict[curr_code])
    res.append([curr_code] + curr_data)

res = pd.DataFrame(res)
cols = ['code', 'name'] + [f'theme{idx + 1}' for idx in range(res.shape[1] - 2)]
res.columns = cols

res.to_csv('code2theme.csv', index=False, encoding='cp949')

theme_df.to_csv('theme2code.csv', index=False, encoding='cp949')