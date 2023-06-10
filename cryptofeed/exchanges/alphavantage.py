'''
Copyright (C) 2017-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from decimal import Decimal
import logging
from typing import Dict, Tuple
from collections import defaultdict
from time import time
import csv
from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, BUY, ASK, ALPHAVANTAGE, L3_BOOK, SELL, TRADES, TICKER, DAILY_OHLCV, REFRESH_SYMBOL
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.exchanges.mixins.alphavantage_rest import AlphaVantageRestMixin
from typing import Dict, List, Tuple, Union
from cryptofeed.types import Trade, OrderBook, Ticker, RefreshSymbols, OpenHighLowCloseVolume
from datetime import datetime
LOG = logging.getLogger('feedhandler')

class AlphaVantage(Feed, AlphaVantageRestMixin):
    id = ALPHAVANTAGE
    websocket_endpoints = [WebsocketEndpoint('')]
    rest_endpoints = [RestEndpoint('https://www.alphavantage.co/', routes=Routes(['query?function=LISTING_STATUS']))]
    key_seperator = ','
    websocket_channels = {
        TICKER: TICKER,
    }

    rest_channels = {
        DAILY_OHLCV: DAILY_OHLCV,
        REFRESH_SYMBOL: REFRESH_SYMBOL
    }


    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:

        ret = {} 
        info = {'instrument_type': {}, 'key': {}}

        for i in csv.reader(data.splitlines(), delimiter=','):   
            type = i[3]
            if type == 'Stock':
                symbol = i[0]
                
                symbol = symbol.replace('-', '/') 

                s = Symbol(symbol,'USD')
                ret[s.normalized] = str(symbol)
                info['instrument_type'][s.normalized] = s.type

        return ret, info
    
    @classmethod
    def _symbol_endpoint_prepare(cls, ep: RestEndpoint, key_id) -> Union[List[str], str]:
        """
        override if a specific exchange needs to do something first, like query an API
        to get a list of currencies, that are then used to build the list of symbol endpoints
        """
        return [ep + '&apikey=' + key_id for ep in ep.route('instruments')]
    
    async def _refresh_symbol(self, msg, ts):
        for j in msg:
            t = RefreshSymbols(
                self.id, j['symbol'], j['base'], j['quote'], ts, raw=j)
            await self.callback(REFRESH_SYMBOL, t, ts)

    async def refresh_symbol(self):
        data = []
        for j in self.symbols():
            base, quote = j.split('-')
            data.append({'base': base, 'quote': quote, 'symbol': j})

        await self.message_handler({
            'data': json.dumps(data),
            'type': REFRESH_SYMBOL
        }, None, time())

    async def _daily_ohlcv(self, msg, ts):
        for i in msg['Time Series (Daily)'].items():
            t = OpenHighLowCloseVolume(
                self.id,
                self.exchange_symbol_to_std_symbol(msg['Meta Data']['2. Symbol']),
                i[0], # date
                i[1]['1. open'],
                i[1]['2. high'],
                i[1]['3. low'],
                i[1]['4. close'],
                i[1]['6. volume'],
                self._datetime_normalize(datetime.combine(i[0], datetime.min.time())),
            )
            await self.callback(DAILY_OHLCV, t, ts)
            
    async def message_handler(self, msg: str, conn: AsyncConnection, ts: float):
        msg_type = msg.get('type')
        msg = json.loads(msg.get('data'), parse_float=Decimal)

        if msg_type == REFRESH_SYMBOL:
            await self._refresh_symbol(msg, ts)
        if msg_type == DAILY_OHLCV:
            await self._daily_ohlcv(msg, ts)
     
