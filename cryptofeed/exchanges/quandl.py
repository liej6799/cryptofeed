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
from cryptofeed.types import Ticker, RefreshSymbols, OpenHighLowCloseVolume
from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint, HTTPPoll, HTTPAsyncConn
from cryptofeed.defines import BID, BUY, ASK, QUANDL, L3_BOOK, SELL, TRADES, TICKER, DAILY_OHLCV, REFRESH_SYMBOL
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import MissingSequenceNumber
from typing import Dict, List, Tuple, Union
from cryptofeed.exchanges.mixins.quandl_rest import QuandlRestMixin
from datetime import datetime
LOG = logging.getLogger('feedhandler')


class Quandl(Feed, QuandlRestMixin):
    id = QUANDL
    websocket_endpoints = [WebsocketEndpoint('')]
    rest_endpoints = [RestEndpoint(
        'https://static.quandl.com/', routes=Routes(['coverage/WIKI_PRICES.csv']))]
    key_seperator = ','
    websocket_channels = {}

    rest_channels = {
        DAILY_OHLCV: DAILY_OHLCV,
        REFRESH_SYMBOL: REFRESH_SYMBOL
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:

        ret = {}
        info = {'instrument_type': {}, 'key': {}}

        for i in csv.reader(data.splitlines(), delimiter=','):
            symbol = i[0]
            symbol = symbol.replace('_', '/')

            s = Symbol(symbol, 'USD')

            ret[s.normalized] = str(symbol)
            info['instrument_type'][s.normalized] = s.type

        return ret, info

    # def _connect_rest(self):
    #     ret = []
    #     addrs = 'https://data.nasdaq.com/api/v3/datatables/WIKI/PRICES.json?ticker=AAPL&api_key=561eFGtNz8dwM627n-_-'
    #     ret.append((HTTPPoll(addrs, self.id), self.subscribe,
    #                self.message_handler, self.authenticate))

    #     retur`cn ret

    async def _refresh_symbol(self, msg, ts):
        for j in msg:
            t = RefreshSymbols(
                self.id, j['symbol'], j['base'], j['quote'], ts, raw=j)
            await self.callback(REFRESH_SYMBOL, t, ts)

    async def _daily_ohlcv(self, msg, ts):

        for i in msg['datatable']['data']:
            t = OpenHighLowCloseVolume(
                self.id,
                self.exchange_symbol_to_std_symbol(i[0]),
                i[1],
                i[2],
                i[3],
                i[4],
                i[5],
                i[6],
                self._datetime_normalize(datetime.combine(i[1], datetime.min.time())),
            )
            await self.callback(DAILY_OHLCV, t, ts)

    async def message_handler(self, msg: str, conn: AsyncConnection, ts: float):
        msg_type = msg.get('type')
        msg = json.loads(msg.get('data'), parse_float=Decimal)

        if msg_type == REFRESH_SYMBOL:
            await self._refresh_symbol(msg, ts)
        elif msg_type == DAILY_OHLCV:
            await self._daily_ohlcv(msg, ts)

    async def refresh_symbol(self):
        data = []
        for j in self.symbols():
            base, quote = j.split('-')
            data.append({'base': base, 'quote': quote, 'symbol': j})

        await self.message_handler({
            'data': json.dumps(data),
            'type': REFRESH_SYMBOL
        }, None, time())

    async def subscribe(self, conn: AsyncConnection):
        print('subscribing')
