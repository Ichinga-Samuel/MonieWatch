import asyncio


class TaskQueue:
    def __init__(self, coroutine, args: list[dict] = None, workers=0):
        self.queue = asyncio.Queue()
        self.coroutine = coroutine
        self.args = args or []
        self.workers = workers or len(self.args)
        self.add_all()
        self.tasks = []
        self.results = []

    def add(self, obj: dict):
        self.queue.put_nowait(obj)

    def add_all(self):
        [self.queue.put_nowait(obj) for obj in self.args]

    async def worker(self):
        while True:
            obj = await self.queue.get()
            res = await self.coroutine(**obj)
            self.results.append(res)
            self.queue.task_done()

    def create_tasks(self):
        self.tasks.extend(asyncio.create_task(self.worker()) for _ in range(self.workers or 5))

    async def run(self):
        self.create_tasks()
        await self.queue.join()
        [task.cancel() for task in self.tasks]
        await asyncio.gather(*self.tasks, return_exceptions=True)
