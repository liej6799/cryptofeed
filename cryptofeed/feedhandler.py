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
from cryptofeed.defines import CANDLES,MANAGER,  BID, ASK, PYTH, BLOCKCHAIN, FUNDING, GEMINI, L2_BOOK, L3_BOOK, LIQUIDATIONS, OPEN_INTEREST, PERPETUAL, TICKER, TRADES, INDEX, MANAGER_STREAM, RTTREFRESHSYMBOLS

import sys
import time
from typing import List
from cryptofeed.backends.redis import ManagerStream
from cryptofeed.defines import RTTREFRESHSYMBOLS

from cryptofeed.exchanges.bitdotcom import BitDotCom
from cryptofeed.exchanges.pyth import Pyth

try:
    # unix / macos only
    from signal import SIGHUP
    SIGNALS = (SIGABRT, SIGINT, SIGTERM, SIGHUP)
except ImportError:
    SIGNALS = (SIGABRT, SIGINT, SIGTERM)

from yapic import json

from cryptofeed.config import Config
from cryptofeed.defines import L2_BOOK, MANAGER
from cryptofeed.feed import Feed
from cryptofeed.log import get_logger
from cryptofeed.nbbo import NBBO
from cryptofeed.exchanges import EXCHANGE_MAP
from cryptofeed.types import Ticker, RefreshSymbols
LOG = logging.getLogger('feedhandler')


def setup_signal_handlers(loop):
    """
    This must be run from the loop in the main thread
    """
    def handle_stop_signals(*args):
        raise SystemExit
    if sys.platform.startswith('win'):
        # NOTE: asyncio loop.add_signal_handler() not supported on windows
        for sig in SIGNALS:
            signal.signal(sig, handle_stop_signals)
    else:
        for sig in SIGNALS:
            loop.add_signal_handler(sig, handle_stop_signals)


class FeedHandler:
    def __init__(self, config=None, raw_data_collection=None):
        """
        config: str, dict or None
            if str, absolute path (including file name) of the config file. If not provided, config can also be a dictionary of values, or
            can be None, which will default options. See docs/config.md for more information.
        raw_data_collection: callback (see AsyncFileCallback) or None
            if set, enables collection of raw data from exchanges. ALL https/wss traffic from the exchanges will be collected.
        """
        self.feeds = []
        self.config = Config(config=config)
        self.raw_data_collection = None
        self.running = False
        if raw_data_collection:
            Connection.raw_data_callback = raw_data_collection
            self.raw_data_collection = raw_data_collection

        if not self.config.log.disabled:
            get_logger('feedhandler', self.config.log.filename, self.config.log.level)

        if self.config.log_msg:
            LOG.info(self.config.log_msg)

        if self.config.uvloop:
            try:
                import uvloop
                asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
                LOG.info('FH: uvloop initalized')
            except ImportError:
                LOG.info("FH: uvloop not initialized")

    def add_feed(self, feed, loop=None, **kwargs):
        """
        feed: str or class
            the feed (exchange) to add to the handler
        loop: event loop
            the event loop to use for the feed (only when the feedhandler is running)
        kwargs: dict
            if a string is used for the feed, kwargs will be passed to the
            newly instantiated object
        """
        if isinstance(feed, str):
            if feed in EXCHANGE_MAP:
                self.feeds.append((EXCHANGE_MAP[feed](config=self.config, **kwargs)))
            else:
                raise ValueError("Invalid feed specified")
        else:
            self.feeds.append((feed))
        if self.raw_data_collection:
            self.raw_data_collection.write_header(self.feeds[-1].id, json.dumps(self.feeds[-1]._feed_config))
  
        if loop is None:
            loop = asyncio.get_event_loop()

        self.feeds[-1].start(loop)

    def add_nbbo(self, feeds: List[Feed], symbols: List[str], callback, config=None):
        """
        feeds: list of feed classes
            list of feeds (exchanges) that comprises the NBBO
        symbols: list str
            the trading symbols
        callback: function pointer
            the callback to be invoked when a new tick is calculated for the NBBO
        config: dict, str, or None
            optional information to pass to each exchange that is part of the NBBO feed
        """
        cb = NBBO(callback, symbols)
        for feed in feeds:
            self.add_feed(feed(channels=[L2_BOOK], symbols=symbols, callbacks={L2_BOOK: cb}, config=config))

    def run(self, start_loop: bool = True, install_signal_handlers: bool = True, exception_handler=None):
        """
        start_loop: bool, default True
            if false, will not start the event loop.
        install_signal_handlers: bool, default True
            if True, will install the signal handlers on the event loop. This
            can only be done from the main thread's loop, so if running cryptofeed on
            a child thread, this must be set to false, and setup_signal_handlers must
            be called from the main/parent thread's event loop
        exception_handler: asyncio exception handler function pointer
            a custom exception handler for asyncio
        """
        self.running = True
        loop = asyncio.get_event_loop()
        # Good to enable when debugging or without code change: export PYTHONASYNCIODEBUG=1)
        # loop.set_debug(True)

        if install_signal_handlers:
            setup_signal_handlers(loop)

        self.setup_manager_handlers(loop)
   

        for feed in self.feeds:
            feed.start(loop)

        if not start_loop:
            return

        try:
            if exception_handler:
                loop.set_exception_handler(exception_handler)
            loop.run_forever()
        except SystemExit:
            LOG.info('FH: System Exit received - shutting down')
        except Exception as why:
            LOG.exception('FH: Unhandled %r - shutting down', why)
        finally:
            self.stop(loop=loop)
            self.close(loop=loop)

        LOG.info('FH: leaving run()')

    def _stop(self, loop=None):
        self.running = False
        if not loop:
            loop = asyncio.get_event_loop()

        LOG.info('FH: shutdown connections handlers in feeds')
        for feed in self.feeds:
            feed.stop()

        if self.raw_data_collection:
            LOG.info('FH: shutting down raw data collection')
            self.raw_data_collection.stop()

        LOG.info('FH: create the tasks to properly shutdown the backends (to flush the local cache)')
        shutdown_tasks = []
        for feed in self.feeds:
            task = loop.create_task(feed.shutdown())
            try:
                task.set_name(f'shutdown_feed_{feed.id}')
            except AttributeError:
                # set_name only in 3.8+
                pass
            shutdown_tasks.append(task)

        LOG.info('FH: wait %s backend tasks until termination', len(shutdown_tasks))
        return shutdown_tasks

    async def stop_async(self, loop=None):
        shutdown_tasks = self._stop(loop=loop)
        await asyncio.gather(*shutdown_tasks)

    def stop(self, loop=None):
        shutdown_tasks = self._stop(loop=loop)
        loop.run_until_complete(asyncio.gather(*shutdown_tasks))

    def close(self, loop=None):
        """Stop the asynchronous generators and close the event loop."""
        if not loop:
            loop = asyncio.get_event_loop()

        LOG.info('FH: stop the AsyncIO loop')
        loop.stop()
        LOG.info('FH: run the AsyncIO event loop one last time')
        loop.run_forever()

        pending = asyncio.all_tasks(loop=loop)
        LOG.info('FH: cancel the %s pending tasks', len(pending))
        for task in pending:
            task.cancel()

        LOG.info('FH: run the pending tasks until complete')
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))

        LOG.info('FH: shutdown asynchronous generators')
        loop.run_until_complete(loop.shutdown_asyncgens())

        LOG.info('FH: close the AsyncIO loop')
        loop.close()

    def Convert_dict(self, a):  
        init = iter(a)  
        res_dct = dict(zip(init, init))  
        return res_dct  
    async def redis_handler(self, loop):
        stream = ManagerStream()
        stream.start(loop)
        print('redis_handler')
        
        self.add_feed(Pyth(channels=[TICKER], symbols=['ETH-USD']))
      
        await self.stop_async(loop)
        
        self.add_feed(Pyth(channels=[TICKER], symbols=['BTC-USD']))

 
        #self.run()
        while stream.running:
            async with stream.read_queue() as updates:
                update = list(updates)[-1]
                
                if update:
                  
                    decoded = update['data'].decode('UTF-8')
                    if decoded == RTTREFRESHSYMBOLS:
                        #self.feeds.append(Pyth(symbols=['BTC-USD'], channels=[TICKER]))
                        await self.stop_async(loop)
                        print(len(self.feeds))
                   
                        self.add_feed(Pyth(channels=[TICKER], symbols=['ETH-USD']))
                       
 
                        # for i in self.feeds:
                        #     for j in i.symbols(): 
                        #         base, quote = j.split('-')
                        #         t = RefreshSymbols(i.id, base, quote, time.time(), raw=j)
                        #         await i.callback(RTTREFRESHSYMBOLS,t, time.time())
   
    async def manager_handler(self):
        while True:
            try:
                for i in self.feeds:
                    
                    manager = i.manager()
                    data = {
                        'id': manager['id'],
                        'initialized_timestamp': manager['initialized_timestamp'],
                    }
                    # temporary use of Ticker class
                    t = Ticker(i.id, 'pair', 0, 0, manager['initialized_timestamp'], raw=data)

                    await i.callback(MANAGER,t, time.time())
            except Exception:
                pass
            await asyncio.sleep(1)

    def setup_manager_handlers(self, loop):
        loop.create_task(self.manager_handler())
        loop.create_task(self.redis_handler(loop))
