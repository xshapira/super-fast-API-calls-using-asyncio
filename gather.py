import http.client
import asyncio
import json
import time
from typing import Coroutine, Callable, Awaitable, TypedDict
import signal

from base import User, Story, Comment, Poll, PollOption, BaseClass

models: dict[str: BaseClass | User] = {'story': Story, 'comment': Comment, 'poll': Poll, 'pollopt': PollOption,
                                       'job': Story, 'user': User}

lock = asyncio.Lock()


class QueueItem:
    def __init__(self, coroutine: Callable, *args, **kwargs):
        self.coroutine = coroutine
        self.args = args
        self.kwargs = kwargs
    
    async def task(self):
        try:
            return await self.coroutine(*self.args, **self.kwargs)
        except Exception as err:
            print('err')


class TaskQueue:
    def __init__(self, size=0, workers=0, timeout=None):
        self.queue = asyncio.Queue(maxsize=size)
        self.size = size
        self.workers = workers
        self.tasks = []
        self.kill = False
        self.timeout = timeout
    
    def add(self, item: QueueItem):
        try:
            self.queue.put_nowait(item) if not self.kill else ...
        except asyncio.QueueFull:
            return
        
    async def join(self):
        try:
            signal.signal(signal.SIGINT, self.sig_handler)
            signal.signal(signal.SIGTERM, self.sig_handler)
            await asyncio.wait_for(self.queue.join(), timeout=self.timeout)
        except TimeoutError:
            self.kill = True
    
    async def worker(self):
        while True:
            try:
                item: QueueItem = self.queue.get_nowait()
                await item.task()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break
    
    def cleanup(self):
        while self.queue.qsize():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty():
                break
        for task in self.tasks:
            task.cancel()
    
    async def run(self):
        workers = self.workers or self.size or self.queue.qsize()
        self.tasks.extend(asyncio.create_task(self.worker()) for _ in range(workers))
        await self.join()
        await self.cancel()
    
    def sig_handler(self, sig, frame):
        self.kill = True
    
    async def cancel(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)


class API:
    api = 'hacker-news.firebaseio.com'
    version = 'v0'
    url = f'{api}/{version}'
    payload = {}
    connections = set()
    task_queue = TaskQueue(workers=1000, timeout=60)
    result = {
        'ids': set(),
        'data': {}
    }
    
    async def get(self, *, path: str, payload: dict = None):
        conn = http.client.HTTPSConnection('hacker-news.firebaseio.com')
        # self.connections.add(self.conn)
        payload = payload or self.payload
        path = f'/{self.version}/{path}'
        await asyncio.to_thread(conn.request, 'GET', path, payload)
        res = await asyncio.to_thread(conn.getresponse)
        res = json.loads(res.read().decode('utf-8'))
        return res
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def close(self):
        [await asyncio.to_thread(conn.close) for conn in self.connections]
    
    async def save(self, *, data: dict, key: str = ''):
        key = data.get('type', key)
        model = models[key]
        data = model(**data)
        await data.save(self.result['data'])
        self.result['ids'].add(data.id)
    
    async def get_by_id(self, *, item_id):
        if item_id in self.result['ids']:
            return
        path = f'item/{item_id}.json'
        res = await self.get(path=path)
        await self.save(data=res)
        if 'kids' in res:
            [self.task_queue.add(QueueItem(self.get_by_id, item_id=item)) for item in res['kids']]
            
        if 'by' in res:
            self.task_queue.add(QueueItem(self.get_user, user_id=res['by']))
        return res
    
    async def get_user(self, *, user_id):
        if user_id in self.result['ids']:
            return
        
        path = f'user/{user_id}.json'
        res = await self.get(path=path)
        await self.save(data=res, key='user')
    
    async def max_item(self):
        path = 'maxitem.json'
        res = await self.get(path=path)
        return res
    
    async def top_stories(self):
        path = 'topstories.json'
        res = await self.get(path=path)
        return res
    
    async def ask_stories(self):
        path = 'askstories.json'
        res = await self.get(path=path)
        return res
    
    async def job_stories(self):
        path = 'jobstories.json'
        res = await self.get(path=path)
        return res
    
    async def show_stories(self):
        path = 'showstories.json'
        res = await self.get(path=path)
        return res
    
    async def updates(self):
        path = 'updates.json'
        res = await self.get(path=path)
        return res
    
    async def stories(self):
        start = time.process_time()
        s, j, t, a = await asyncio.gather(self.show_stories(), self.job_stories(), self.top_stories(),
                                          self.ask_stories())
        stories = set(s) | set(j) | set(t) | set(a)
        [self.task_queue.add(QueueItem(self.get_by_id, item_id=itd)) for itd in stories]
        await self.task_queue.run()
        print(len(self.result['data']))
        print(time.process_time() - start)
        # print(len(self.connections))


asyncio.run(API().stories())
