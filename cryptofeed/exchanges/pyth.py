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

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, BUY, ASK, PYTH, L3_BOOK, SELL, TRADES, TICKER, REFRESH_SYMBOL
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.exceptions import MissingSequenceNumber
from cryptofeed.types import Trade, OrderBook, Ticker, RefreshSymbols
       
from pythclient.solana import SolanaAccount, SolanaClient, PYTHNET_HTTP_ENDPOINT, PYTHNET_WS_ENDPOINT, SolanaPublicKey
from pythclient.pythaccounts import PythAccount, PythPriceAccount
from pythclient.pythclient import WatchSession

LOG = logging.getLogger('feedhandler')


class Pyth(Feed):
    id = PYTH
    websocket_endpoints = [WebsocketEndpoint(PYTHNET_WS_ENDPOINT)]
    rest_endpoints = [RestEndpoint(PYTHNET_HTTP_ENDPOINT, routes=Routes(['']))]
    key_seperator = ','
    websocket_channels = {
        TICKER: 'ticker',
    }
    rest_channels = {
        REFRESH_SYMBOL: REFRESH_SYMBOL
    }
    solana_client = SolanaClient(endpoint=PYTHNET_HTTP_ENDPOINT, ws_endpoint=PYTHNET_WS_ENDPOINT)
        
    def __reset(cls):
        cls._l3_book = {}
        cls._order_ids = defaultdict(dict)
        cls._sequence_no = {}

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:

        ret = {}
        info = {'instrument_type': {}, 'key': {}}
        

        for i in data:
            symbol = i['symbol']
            key = i['key']

            symbol = symbol.replace('Crypto.', '') 
            symbol = symbol.replace('Equity.US.', '') 
            symbol = symbol.replace('Equity.GB.', '') 
            symbol = symbol.replace('FX.', '') 
            symbol = symbol.replace('METAL.', '') 

            if '/' in symbol:
                base, quote = symbol.split("/")

            s = Symbol(base,quote)
            ret[s.normalized] = str(symbol) + cls.key_seperator + str(key)
            info['instrument_type'][s.normalized] = s.type
            info['key'][s.normalized] = key

        return ret, info
    
    
    @classmethod
    def _get_symbol_data(cls, key_id = None):
        return asyncio.get_event_loop().run_until_complete(cls.get_all_symbols_async())

    @classmethod
    async def get_all_symbols_async(cls):

        from pythclient.pythclient import PythClient
        from pythclient.utils import get_key # noqa

        v2_first_mapping_account_key = get_key("pythnet", "mapping")
        v2_program_key = get_key("pythnet", "program")

        async with PythClient(
            first_mapping_account_key=v2_first_mapping_account_key,
            program_key=v2_program_key,
            solana_endpoint=PYTHNET_HTTP_ENDPOINT, # replace with the relevant cluster endpoints
            solana_ws_endpoint=PYTHNET_WS_ENDPOINT # replace with the relevant cluster endpoints
        ) as c:
            result = []
            await c.refresh_all_prices()
            products = await c.get_products()
            for p in products:
                prices = await p.get_prices()
                for _, pr in prices.items():
                    result.append({'symbol': pr.product.symbol, 'key':pr.key})

            return result
        
    async def _refresh_symbol(self, msg, ts):
        for j in msg.get('data'):
            t = RefreshSymbols(
                self.id, j['symbol'], j['base'], j['quote'], ts, raw=j)
            await self.callback(REFRESH_SYMBOL, t, time())
    
    async def _ticker(cls, msg: dict, timestamp: float):
        """
        {
            'LENGTH': 32, 
            'raw_price': 2683541666667, 
            'raw_confidence_interval': 1032666667, 
            'price_status': 1, 
            'pub_slot': 68764081, 
            'exponent': -8, 
            'price': Decimal('26835.41666667'), 
            'confidence_interval': Decimal('10.32666667')
        }
        """

        pair = cls.exchange_symbol_to_std_symbol(msg['symbol'] + cls.key_seperator + str(msg['key']))
        bid = Decimal(msg['aggregate_price_info']['price'])
        ask = Decimal(msg['aggregate_price_info']['price'])

        if 'timestamp' in msg:
            ts = float(msg['timestamp'])
        else:
            ts = timestamp
        
        t = Ticker(cls.id, pair, bid, ask, ts, raw=msg)

        await cls.callback(TICKER, t, timestamp)

    async def _refresh_symbol(self, msg, ts):
        for j in msg:
            t = RefreshSymbols(
                self.id, j['symbol'], j['base'], j['quote'], ts, raw=j)
            await self.callback(REFRESH_SYMBOL, t, time())

    async def refresh_symbol(self):
        data = []
        for j in self.symbols():
            base, quote = j.split('-')
            data.append({'base': base, 'quote': quote, 'symbol': j})
      
        await self.message_handler({
            'data': json.dumps(data),
            'type': REFRESH_SYMBOL
        }, None, time())

    async def message_handler(self, msg: str, conn: AsyncConnection, ts: float):
        msg_type = msg.get('type')
        msg = json.loads(msg.get('data'), parse_float=Decimal)

        if msg_type == REFRESH_SYMBOL:
            await self._refresh_symbol(msg, ts)
        
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(cls, conn: AsyncConnection):
        """
        Temporary put here first if possible to call pyth.network 
        without library can merge into connections
        """
        subs = []
        all_prices = []

        if cls.std_channel_to_exchange(TICKER) in conn.subscription:
        
            for chan, symbols in conn.subscription.items():
                subs.extend([s for s in set([sym.split(",")[1] for sym in symbols])])
            
            for s in subs:
                data = SolanaPublicKey(s)
                all_prices.append(PythPriceAccount(data, cls.solana_client))

            ws = WatchSession(cls.solana_client)

            await ws.connect()
            for account in all_prices:
                await ws.subscribe(account)

            while True:
                if not conn.is_open:
                    break
                update_task = asyncio.create_task(ws.next_update())
                while True:
                    done, _ = await asyncio.wait({update_task}, timeout=1)
                    if update_task in done:
                        pr = update_task.result()
    
                        for sym in symbols:
                            if str(sym.split(cls.key_seperator)[1]) == str(pr.key):
                    
                                data = {
                                    "key" : str(pr.key)
                                    ,'symbol' : sym.split(cls.key_seperator)[0]
                                    ,'exponent' : pr.exponent 
                                    ,'num_components' : pr.num_components 
                                    ,'last_slot' : pr.last_slot 
                                    ,'valid_slot' : pr.valid_slot 
                                    ,'aggregate_price_info' : pr.aggregate_price_info
                                    ,'timestamp' : float(pr.timestamp)
                                    ,'min_publishers' : pr.min_publishers 
                                    ,'prev_slot' : pr.prev_slot 
                                    ,'prev_price' : pr.prev_price 
                                    ,'prev_conf' : pr.prev_conf 
                                    ,'prev_timestamp' : pr.prev_timestamp 
                                } 

                                await cls.message_handler({
                                    'data': json.dumps(data),
                                    'type': TICKER
                                }, conn, time())

                        break

            await ws.disconnect()
