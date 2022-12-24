from datetime import datetime
from tortoise import Tortoise, run_async

from utils.db import TORTOISE_ORM
from models.transaction import TransactionsReport, Transactions, Transaction, Agent


async def connect():
    await Tortoise.init(config=TORTOISE_ORM)
    await Tortoise.generate_schemas(safe=True)


async def make_report():
    trans = [
        Transaction(
            business_name="Test Business",
            time=datetime.today(),
            trans_type="Airtime",
            agent_id=22432,
            amount=455323443
        )
    ]
    agents = [Agent(agent_id=22432, name="Test Agent", mobile=2349037038)]
    report = Transactions(title="Test Report", transactions=trans, agents=agents)
    rep = await report.get_pdf()
    return rep
