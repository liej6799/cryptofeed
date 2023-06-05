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
from cryptofeed.defines import BID, BUY, ASK, ALPHAVANTAGE, L3_BOOK, SELL, TRADES, TICKER, DAILY_OHLCV
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.exchanges.mixins.alphavantage_rest import AlphaVantageRestMixin
from typing import Dict, List, Tuple, Union

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
        DAILY_OHLCV: DAILY_OHLCV
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
    
    async def message_handler(self, msg: str, conn: AsyncConnection, ts: float):
        print(msg, conn, ts)
