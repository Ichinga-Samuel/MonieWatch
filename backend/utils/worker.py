from logging import getLogger
import datetime

from celery import Celery
from tortoise import run_async

from .functions import get_report as gr, run
from .env import env

from models.aggregator import Aggregator, Agent

logger = getLogger()

app = Celery('workers', broker=env.celery_broker_url, backend="rpc://")


@app.task(name="get_agents")
async def get_agents(data: dict):
    aggregator = Aggregator.parse_obj(data)
    cor = aggregator.init()
    run_async(run(cor))


@app.task(name='get_reports')
def get_report(agg: dict, data: dict):
    try:
        aggregator = Aggregator.parse_obj(agg)
        data['agents'] = [Agent.parse_obj(obj) for obj in data['agents']] if data['agents'] else None
        data['start_date'] = datetime.datetime.strptime(data['start_date'].split("T")[0], "%Y-%m-%d")
        data['end_date'] = datetime.datetime.strptime(data['end_date'].split("T")[0], "%Y-%m-%d")
        coro = gr(aggregator=aggregator, **data)
        run_async(run(coro))
    except Exception as exc:
        logger.error(exc)
