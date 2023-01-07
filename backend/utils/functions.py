from logging import getLogger
from datetime import date
import asyncio

from tortoise import Tortoise

from models.aggregator import Aggregator, Agent
from models.tables_orm import AggregatorORM, ReportORM
from .task_queue import TaskQueue
from .db import TORTOISE_ORM

logger = getLogger()


async def get_aggregators() -> list[Aggregator]:
    aggregators = await AggregatorORM.all()
    return [Aggregator.from_orm(i) for i in aggregators if i]


async def connect():
    await Tortoise.init(config=TORTOISE_ORM)


async def generate_reports(*, start_date: date | None = None, end_date: date | None = None, aggregators: list[Aggregator] | None = None):
    aggregators = aggregators or await get_aggregators()
    args = [{'aggregator': aggregator, 'start_date': start_date, 'end_date': end_date} for aggregator in aggregators]
    tasks = TaskQueue(coroutine=generate_report, args=args)
    await tasks.run()


async def get_report(*, aggregator: Aggregator, start_date: date | None = None, end_date: date | None = None, target: None | float = None,
                          agents: list[Agent] | None = None, title: str = ""):

    report = await generate_report(aggregator=aggregator, start_date=start_date, end_date=end_date, target=target, agents=agents, title=title)
    await aggregator.send_report(url=report.url)


async def generate_report(*, aggregator: Aggregator, start_date: date | None = None, end_date: date | None = None, target: None | float = None,
                          agents: list[Agent] | None = None, title: str = "") -> ReportORM:
    try:
        trans = await aggregator.get_transactions(start_date=start_date, end_date=end_date, target=target, agents=agents, title=title)
        file = await aggregator.get_pdf(transactions=trans)
        res = await aggregator.upload_to_cloud(file=file)
        return await aggregator.save_report(**res)
    except Exception as err:
        logger.critical(f"{err}: Unable to generate report")


async def run(*coroutines):
    try:
        await connect()
        await asyncio.gather(*coroutines, return_exceptions=True)
    except Exception as err:
        logger.warning(err)
