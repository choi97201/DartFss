import cjw_maria
import pandas as pd
import datetime

def play():
    host = "15.165.29.213"
    user = "lt_user"
    password = "De4IjOY32e7o" 
    db = "leaderstrading"
    maria = cjw_maria.MariaDB(host, user, password, db)

    sql = 'SELECT NAME, ID, ClientID, CompleteTime, StrategyName, TradingType, Status \
        FROM trading_history \
        ORDER BY ClientID, NAME, CompleteTime ASC;'
    ordered_df = maria.showData(sql)
    
    after_processed_df = ordered_df.drop_duplicates(subset=['NAME', 'ClientID'], keep='last')

    after_processed_df.value_counts('TradingType')

    have_df = after_processed_df[after_processed_df['TradingType']=='매수'].reset_index(drop=True)
    have_df.drop_duplicates(subset=['ID', 'NAME'])[['ID', 'NAME']].to_csv('history_name.csv', index=False, encoding='utf-8-sig')
    
    df = have_df[['ClientID', 'NAME', 'StrategyName']]
    
    df['NAME'] = pd.DataFrame(df['NAME']).applymap(str.upper)['NAME']
    tmp = list(df['NAME'])
    df['NAME'] = [t.replace(' ','') for t in tmp]
    
    boyoo = pd.read_excel('보유종목.xlsx', sheet_name='보유종목')
    boyoo.columns = boyoo.iloc[3]
    boyoo = boyoo.iloc[5:]
    boyoo.columns = [c.replace(' ','') if type(c)==str else c for c in list(boyoo.columns)]

    tmp = ['VVIP1', 'SVIP6', 'G', 'E', 'A', 'C', 'S', 'N', 'T', 'S\'']

    cols = []
    for i in range(len(boyoo.columns)):
        if boyoo.columns[i] in tmp:
            cols.append(boyoo.columns[i])
            cols.append(boyoo.columns[i+1])



    boyoo = boyoo[cols]
    boyoo.columns
    woonyong = []

    for i in range(0, len(list(boyoo.columns)), 2):
        data = list(boyoo[boyoo.columns[i:i+2]].dropna()[boyoo.columns[i]])
        for d in data:
            if len(boyoo.columns[i])<=2:
                woonyong.append(['Master.'+boyoo.columns[i], d])
            else:
                woonyong.append([boyoo.columns[i], d])

    woonyong = pd.DataFrame(woonyong, columns=['전략명', '종목명'])

    woonyong['종목명'] = pd.DataFrame(woonyong['종목명']).applymap(str.upper)['종목명']
    tmp = list(woonyong['종목명'])
    woonyong['종목명'] = [t.replace(' ','') for t in tmp]
    woonyong = woonyong.reset_index(drop=True)

    u_start = woonyong[woonyong['종목명']=='U'].index[0]


    tmp = woonyong.iloc[u_start:].reset_index(drop=True)

    for i in range(1, tmp.shape[0]):
        if tmp['전략명'][i] != tmp['전략명'][i-1]:
            break

    woonyong = woonyong.drop(u_start)
    woonyong = woonyong.drop(u_start+1)
    woonyong.iloc[u_start:u_start+i-2]['전략명'] = ['Master.U'] * (i-2)
    woonyong = woonyong.reset_index(drop=True)

    nowtime = '보유종목'+str(datetime.datetime.now()).replace(' ', '_').replace(':', '.') + '.csv'
    woonyong.to_csv(nowtime, encoding='utf-8-sig', index=False)

    woonyong['종목명'] = pd.DataFrame(woonyong['종목명']).applymap(str.upper)['종목명']
    tmp = list(woonyong['종목명'])
    woonyong['종목명'] = [t.replace(' ','') for t in tmp]
    woonyong = woonyong.reset_index(drop=True)
    
    error_name = []
    for i in range(woonyong.shape[0]):
        if woonyong['종목명'][i] not in list(df.drop_duplicates('NAME')['NAME']):
            error_name.append(woonyong['종목명'][i])

    pd.DataFrame(error_name, columns=['종목명']).to_csv('error_name.csv', index=False, encoding='utf-8-sig')

    stradegies = ['Master.A','Master.C','Master.E',
                  'Master.S','Master.U','Master.N',
                  'Master.G','Master.T','SVIP6', 'VVIP1']
    
    indexNames = []
    for i in range(woonyong['전략명'].shape[0]):
        if woonyong['전략명'].iloc[i] in stradegies:
            indexNames.append(i)
    
    
    woonyong = woonyong.iloc[indexNames].reset_index(drop=True)
    
    res = []
    
    df['new'] = df['StrategyName'].astype(str) + df['NAME']
    
    woonyong = woonyong.replace('Master.S\'', 'Master.S')
    woonyong['new'] = woonyong['전략명'].astype(str) + woonyong['종목명']
    
    for n in list(df.drop_duplicates('new')['new']):
        if n not in list(woonyong['new']):
            res.append(df[df['new']==n][['NAME', 'ClientID', 'StrategyName']])
            
    df = pd.concat(res)
    
    sql = "SELECT A.USER_ID, A.USER_NAME, B.STRPURSTARTTIME \
    FROM TRM2200 AS A LEFT OUTER JOIN TRM2300 B \
    ON A.SEQ = B.TRM2200_SEQ \
    LEFT OUTER JOIN TRM1300 C \
    ON B.TRM1300_SEQ = C.STRSEQ \
    LEFT OUTER JOIN TRM1310 D \
    ON D.STRSEQ = C.STRSEQ \
    LEFT OUTER JOIN BASECODE E \
    ON E.TOTAL_CODE = D.STRRANGECODE \
    LEFT OUTER JOIN BASECODE F \
    ON F.TOTAL_CODE = B.STRPURCODE \
    LEFT OUTER JOIN BASECODE G \
    ON G.TOTAL_CODE = B.STRSELLCODE \
    LEFT OUTER JOIN BASECODE H \
    ON H.TOTAL_CODE = B.STRPURVAL \
    LEFT OUTER JOIN BASECODE I \
    ON I.TOTAL_CODE = B.STRSELLVAL \
    WHERE STRNAME='Master.G';"

    clients = maria.showData(sql).drop_duplicates(subset='USER_ID', keep='last')
    time_list = list(clients['STRPURSTARTTIME'])
    time_list = ['0'+t if len(t)==4 else t for t in time_list]
    clients['STRPURSTARTTIME'] = time_list
    clients = clients[clients['STRPURSTARTTIME'] < '15:30'][['USER_ID', 'USER_NAME']]
    clients.columns = ['ClientID', 'ClientName']
    
    df = pd.merge(df, clients, on='ClientID')[['ClientID', 'ClientName', 'StrategyName', 'NAME']]
    
    nowtime = str(datetime.datetime.now()).replace(' ', '_').replace(':', '.') + '.csv'
    df.to_csv(nowtime, encoding='utf-8-sig', index=False)
    return df

    
if __name__ == '__main__':
    df = play()
    
    






