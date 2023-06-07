'''
Copyright (C) 2017-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio

from cryptofeed.connection import Connection
import logging
import signal
from signal import SIGABRT, SIGINT, SIGTERM
from cryptofeed.defines import CANDLES,MANAGER,  BID, ASK, PYTH, BLOCKCHAIN, FUNDING, GEMINI, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, PERPETUAL, TICKER, TRADES, INDEX, MANAGER_STREAM, RTTREFRESHSYMBOLS, DAILY_OHLCV

import sys
import time
from typing import List
from cryptofeed.backends.redis import ManagerStream
from cryptofeed.defines import RTTREFRESHSYMBOLS

from cryptofeed.exchanges.bitdotcom import BitDotCom
from cryptofeed.exchanges.pyth import Pyth
from cryptofeed.exchanges.alphavantage import AlphaVantage
from cryptofeed.exchanges.quandl import Quandl
from cryptofeed.backends.appwrite import RTTRefreshSymbolAppwrite
from cryptofeed.backends.postgres import SymbolPostgres
from cryptofeed.types import Ticker, RefreshSymbols
from cryptofeed.feedhandler import FeedHandler

class Manager(FeedHandler):

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)
    
    async def stop_feed(self, loop):
        await self.stop_async(loop)

    def start_feed(self, loop):
        self.start(loop)

    async def restart_feed(self, loop):
        await self.stop_feed(loop)
        self.start_feed(loop)

    def refresh_symbols(self, loop):
         # fire and forget
        loop.create_task(self.feeds[-1].refresh_symbols())

    def daily_ohlcv_sync(self, loop):
        # fire and forget
        loop.create_task(self.feeds[-1][DAILY_OHLCV]('AAPL-USD'))

    async def ticker(self, t, receipt_timestamp):
        print(f'Ticker received at {receipt_timestamp}: {t}')

    def setup_manager(self, loop):

        self.add_feed(Quandl(loop=loop, symbols=['AAPL-USD'], channels=[DAILY_OHLCV], config=self.config,
                        callbacks={RTTREFRESHSYMBOLS: self.ticker} ))

        # print(self.feeds[-1].daily_ohlcv_sync('TSLA-USD'))
        # once task is created, cant perform run_until_complete, use await instead
        self.refresh_symbols(loop)

        #self.daily_ohlcv_sync(loop)
  