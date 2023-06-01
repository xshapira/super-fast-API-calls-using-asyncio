import asyncio
from typing import Coroutine, Callable, Awaitable


class QueueItem:
    def __init__(self, coroutine: Callable | Awaitable | Coroutine, *args, **kwargs):
        self.coroutine = coroutine
        self.args = args
        self.kwargs = kwargs

    async def run(self):
        try:
            return await self.coroutine(*self.args, **self.kwargs)
        except Exception as err:
            print('err')


class TaskQueue:
    def __init__(self, size=0, workers=0):
        self.queue = asyncio.Queue(maxsize=size)
        self.workers = workers
        self.tasks = []

    def add(self, item: QueueItem):
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            return

    async def worker(self):
        while True:
            try:
                item: QueueItem = self.queue.get_nowait()
                await item.run()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break

    async def run(self):
        workers = self.workers or self.queue.qsize()
        self.tasks.extend(asyncio.create_task(self.worker()) for _ in range(workers))
        await self.queue.join()
        # await self.cancel()

    async def cancel(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
