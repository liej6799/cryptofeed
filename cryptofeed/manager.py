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

    async def redis_handler(self, loop):
        stream = ManagerStream()
        stream.start(loop)
        while stream.running:
            async with stream.read_queue() as updates:
                update = list(updates)[-1]
                
                if update:
                  
                    decoded = update['data'].decode('UTF-8')
                    if decoded == RTTREFRESHSYMBOLS:
                        self.refresh_symbols(loop)
                        break
                    elif decoded == DAILY_OHLCV:
                        self.daily_ohlcv(loop)
                        break

    def refresh_symbols(self, loop):
         # fire and forget
        for i in self.feeds:                
            loop.create_task(i[RTTREFRESHSYMBOLS]())

    def daily_ohlcv(self, loop):
        # fire and forget
        for i in self.feeds:                
            loop.create_task(i[DAILY_OHLCV]())
        
    async def ticker(self, t, receipt_timestamp):
        print(f'Ticker received at {receipt_timestamp}: {t}')

    def setup_manager(self, loop):

        self.add_feed(Quandl(loop=loop, symbols=['AAPL-USD'], channels=[DAILY_OHLCV], config=self.config,
                        callbacks={RTTREFRESHSYMBOLS: self.ticker} ))

        # print(self.feeds[-1].daily_ohlcv_sync('TSLA-USD'))
        # once task is created, cant perform run_until_complete, use await instead
        self.daily_ohlcv(loop)
        #loop.create_task(self.redis_handler(loop))

        #self.daily_ohlcv_sync(loop)
  