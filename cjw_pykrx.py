from pykrx import stock

def getStockCode(market='KOSPI'):
    return stock.get_market_ticker_list(market=market)
    