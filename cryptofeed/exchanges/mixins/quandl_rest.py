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

from cryptofeed.defines import DELETE, GET, POST, DAILY_OHLCV
from cryptofeed.exchange import RestExchange
from cryptofeed.types import Candle


LOG = logging.getLogger('feedhandler')


class QuandlRestMixin(RestExchange):
    api = "https://data.nasdaq.com/api/v3/datatables/"
    rest_channels = (
        DAILY_OHLCV
    )

    async def _request(self, method: str, endpoint: str, auth: bool = False, payload={}, api=None, type=None):

        query_string = urlencode(payload)

        if auth:
            if query_string:
                query_string = '{}&api_key={}'.format(
                    query_string, self.key_id)
            else:
                query_string = 'api_key={}'.format(self.key_id)

        if not api:
            api = self.api

        url = f'{api}{endpoint}?{query_string}'

        header = {}
        if method == GET:
            data = await self.http_conn.read(url, header=header, type=type)
        elif method == POST:
            data = await self.http_conn.write(url, msg=None, header=header, type=type)
        elif method == DELETE:
            data = await self.http_conn.delete(url, header=header, type=type)

        return data

    async def daily_ohlcv(self):

        # loop through all symbols and get daily ohlcv
        if self.std_channel_to_exchange(DAILY_OHLCV) in self.subscription:
            for pair in self.subscription[DAILY_OHLCV]:
                await self._request(GET, 'WIKI/PRICES.json', auth=True, payload={'ticker': (pair)}, type=DAILY_OHLCV)
        
