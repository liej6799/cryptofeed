APPWRITE_ADDR = 'https://192.168.191.213:4430'
APPWRITE_PROJ = '6469dde52fe9831c4b94'
APPWRITE_KEY = '548dc4eda5e039d46a7bb3fd8ee0c9bff427403e135a2bad71bde34e350c5b022fc8cdad1892a2f84b628ee31457c764f179e56b963354c933d8a3f4b1fbfda1b9728b9e12a01ef9d44def2d6387cd4a59dce87152d877e4640aff442a04faebe091fd2e2cc9cf3248f274ba51c8cc4d6aef7480c5eac9b4fe593b6349e3e823'


from decimal import Decimal
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES,MANAGER,  BID, ASK, PYTH, BLOCKCHAIN, FUNDING, GEMINI, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, PERPETUAL, TICKER, TRADES, INDEX, MANAGER_STREAM, RTTREFRESHSYMBOLS
from cryptogee
from cryptofeed.exchanges import (Binance, BinanceUS, BinanceFutures, Bitfinex, Bitflyer, AscendEX, Bitmex, Bitstamp, Bittrex, Coinbase, Gateio,
                                  HitBTC, Huobi, HuobiDM, HuobiSwap, Kraken, OKCoin, OKX, Poloniex, Bybit, KuCoin, Bequant, Upbit, Probit, Pyth)


async def ticker(t, receipt_timestamp):
    if t.timestamp is not None:
        assert isinstance(t.timestamp, float)
    assert isinstance(t.exchange, str)
    assert isinstance(t.bid, Decimal)
    assert isinstance(t.ask, Decimal)
    print(f'Ticker received at {receipt_timestamp}: {t}')


def main():
    f = FeedHandler(config='config.yaml')
    f.add_feed(Pyth(callbacks={TICKER: ticker}))
    f.run()


if __name__ == '__main__':
    main()
