'''
Copyright (C) 2017-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
import asyncio
from decimal import Decimal
import hashlib
import hmac
import logging
import time
from urllib.parse import urlencode

from yapic import json

from cryptofeed.defines import BALANCES, BUY, CANCEL_ORDER, CANDLES, DELETE, FILL_OR_KILL, GET, GOOD_TIL_CANCELED, IMMEDIATE_OR_CANCEL, LIMIT, MARKET, ORDERS, ORDER_STATUS, PLACE_ORDER, POSITIONS, POST, SELL, TRADES, DAILY_OHLCV
from cryptofeed.exchange import RestExchange
from cryptofeed.types import Candle


LOG = logging.getLogger('feedhandler')


class AlphaVantageRestMixin(RestExchange):
    api = "https://www.alphavantage.co/"
    rest_channels = (
        DAILY_OHLCV 
    )
    
    async def _request(self, method: str, endpoint: str, auth: bool = False, payload={}, api=None):
        query_string = urlencode(payload)

        if auth:
            if query_string:
                query_string = '{}&apikey={}'.format(query_string, self.key_id)
            else:
                query_string = 'apikey={}'.format(self.key_id)

        if not api:
            api = self.api

        url = f'{api}{endpoint}?{query_string}'
      
        header = {}
        if method == GET:
            data = await self.http_conn.read(url, header=header)
        elif method == POST:
            data = await self.http_conn.write(url, msg=None, header=header)
        elif method == DELETE:
            data = await self.http_conn.delete(url, header=header)
        return json.loads(data, parse_float=Decimal)

    async def daily_ohlcv(self, symbol: str = None):
        data = await self._request(GET, 'query', auth=True, payload={'function': 'TIME_SERIES_DAILY_ADJUSTED', 'symbol': self.std_symbol_to_exchange_symbol(symbol), 'outputsize': 'full'})
        return data
