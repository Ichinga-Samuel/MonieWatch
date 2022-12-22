from logging import getLogger
from datetime import datetime, date
import asyncio

from tortoise import Tortoise, run_async

from models.aggregator import Aggregator, Agent
from models.tables_orm import AggregatorORM, AgentORM
from models.transaction import Transactions
from .db import TORTOISE_ORM

logger = getLogger()


async def get_aggregators() -> list[Aggregator]:
    aggregators = await AggregatorORM.all()
    return [Aggregator.from_orm(i) for i in aggregators if i]


async def connect():
    await Tortoise.init(config=TORTOISE_ORM)
    await Tortoise.generate_schemas(safe=True)


async def add_agent(aggregator: Aggregator):
    return await aggregator.update_agents()


async def generate_reports(*, start_date: date | None = None, end_date: date | None = None, aggregators: list[Aggregator] | None = None):
    aggregators = aggregators or []
    tasks = [generate_report(aggregator=aggregator, start_date=start_date, end_date=end_date) for aggregator in aggregators]
    await asyncio.gather(*tasks)


async def generate_report(*, aggregator: Aggregator, start_date: date | None = None, end_date: date | None = None, target: None | float = None,
                          agents: list[Agent] | None = None) -> bool:
    await aggregator.authenticate()
    return await aggregator.get_transactions(start_date, end_date, target, agents=agents)


async def add_agents(aggregators: list[Aggregator]):
    tasks = [add_agent(aggregator) for aggregator in aggregators]
    await asyncio.gather(*tasks)


async def authenticate_all(aggregators: list[Aggregator]):
    tasks = [aggregator.authenticate() for aggregator in aggregators]
    await asyncio.gather(*tasks)


async def get_aggregator(**query) -> Aggregator:
    """
    :param query: query as keyword args eg. name=boss_man
    :return:
    """
    aggregator = await AggregatorORM.get(**query)
    aggregator = Aggregator.from_orm(aggregator)
    return aggregator


async def run(*coroutines):
    try:
        await connect()
        await asyncio.gather(*coroutines)
    except:
        logger.info('Ok')
