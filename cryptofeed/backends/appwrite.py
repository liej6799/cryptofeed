'''
Copyright (C) 2017-2023 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''

from collections import defaultdict
import logging

from yapic import json

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.backends.http import HTTPCallback
from cryptofeed.defines import BID, ASK, SYMBOL

LOG = logging.getLogger('feedhandler')

# DATABASES, COLLECTIONS, APPWRITE_PROJ, APPWRITE_KEY
class AppwriteCallback(HTTPCallback):
    def __init__(self, addr: str, project: str, token:str, key=None, **kwargs):
       
        super().__init__(addr, **kwargs)
        self.addr = f"{addr}/v1/functions/{self.functionId}/executions"
        self.headers = {"X-Appwrite-Key": f"{token}",
                        "X-Appwrite-Project": f"{project}",
                        "Content-Type": "application/json"}

        self.session = None
        self.key = key if key else self.default_key
        self.numeric_type = float
        self.none_to = None
        self.running = True
    

    async def writer(self):
       
        while self.running:
            async with self.read_queue() as updates:
                for update in updates:
                    data = self.parse(update)
                    
                    await self.http_write(json.dumps(data), headers=self.headers)
                    
        await self.session.close()

class SymbolAppwrite(AppwriteCallback, BackendCallback):
    default_key = SYMBOL
    database_id = "rtt-db"
    collection_id = "tb_symbol"

    def __init__(self, *args, **kwargs):
        self.functionId = 'rtt-upsert-db'
        super().__init__(*args, **kwargs)
        
    def parse(self, data: str):
        return { "async": True, "data": json.dumps(
            { "database_id": self.database_id, "collection_id": self.collection_id, "data": json.dumps(data)}
        )}
    

    
    
