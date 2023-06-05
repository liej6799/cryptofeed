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
from cryptofeed.defines import BID, BUY, ASK, QUANDL, L3_BOOK, SELL, TRADES, TICKER, DAILY_OHLCV
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import MissingSequenceNumber
from typing import Dict, List, Tuple, Union
from cryptofeed.exchanges.mixins.quandl_rest import QuandlRestMixin
LOG = logging.getLogger('feedhandler')

class Quandl(Feed, QuandlRestMixin):
    id = QUANDL
    websocket_endpoints = [WebsocketEndpoint('')]
    rest_endpoints = [RestEndpoint('https://static.quandl.com/', routes=Routes(['coverage/WIKI_PRICES.csv']))]
    key_seperator = ','
    websocket_channels = {}

    rest_channels = {
        DAILY_OHLCV: DAILY_OHLCV
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
 
        ret = {} 
        info = {'instrument_type': {}, 'key': {}}

        for i in csv.reader(data.splitlines(), delimiter=','):  
            symbol = i[0] 
            symbol = symbol.replace('_', '/') 

            s = Symbol(symbol,'USD')

            ret[s.normalized] = str(symbol)
            info['instrument_type'][s.normalized] = s.type

        return ret, info

    async def message_handler(self, msg: str, conn: AsyncConnection, ts: float):
        print(msg, conn, ts)
