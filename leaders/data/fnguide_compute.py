from fnguide import *

periods = ['2021_03']
buy_days = ['20210618']
sell_days = ['20210618']

fnguide = Data()
fnguide.setMaria()
fnguide.computeValue('value', periods, buy_days, sell_days)
