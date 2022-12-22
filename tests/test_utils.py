import asyncio
from datetime import datetime

from utils.email import ReportEmail
from models.transaction import TransactionsReport, Transactions, Transaction, Agent
from models.tables_orm import ReportORM, AggregatorORM
from models.aggregator import Report
from utils.cloud_upload import S3
from tests import make_report, connect


class TestUtils:

    async def test_email(self):
        res = await ReportEmail(link="https://www.goal.com", name="Samuel", recipients=["ichingasamuel@gmail.com"]).send()
        assert res is True

    async def test_report(self):
        rep = await make_report()
        assert rep.name == "Test Report.pdf"

    async def test_s3(self):
        rep = await make_report()
        s3 = S3()
        res = await s3(file=rep)
        assert res.response.status is True

    async def test_db(self):
        await connect()
        agg = await AggregatorORM.all().first()
        rep = await ReportORM.create(name="Test Report", aggregator=agg, url="https://www.goal.com")
        print(rep.name)


asyncio.run(TestUtils().test_db())
