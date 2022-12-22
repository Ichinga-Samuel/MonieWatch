import asyncio
import datetime
import json
from models.aggregator import Aggregator, AggregatorORM, Agent, ReportORM, Report
from utils.functions import run, generate_report
from tests import connect, run_async


class TestAggregator:
    agg = Aggregator(username="egbezino1@gmail.com", password="Keno@@1989", email="ichingasamuel@gmail.com")

    async def create(self) -> AggregatorORM:
        agg = await AggregatorORM.create(username="egbezino1@gmail.com", password="Keno@@1989", email="ichingasamuel@gmail.com")
        return agg

    async def init(self):
        await connect()
        res = await self.agg.authenticate()
        await self.agg.update_agents()
        await self.agg.profile_update()
        # print(res)
        # await self.agg.init()

    async def test_report(self):
        await generate_report(aggregator=self.agg, agents=None)

    async def test_aggregator(self):
        await connect()
        agg = await AggregatorORM.all().first().prefetch_related('agents', 'reports')
        reports = [Report(**obj).dict() for obj in await agg.reports.all().values()]



asyncio.run(TestAggregator().test_aggregator())
