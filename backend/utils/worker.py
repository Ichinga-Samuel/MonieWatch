from logging import getLogger
import asyncio
import datetime

from celery import Celery

from .functions import add_agents, add_agent, Aggregator, run_async, run, generate_report, Agent
from .env import env

logger = getLogger()

app = Celery('workers', broker=env.celery_broker_url, backend=env.celery_result_backend)


@app.task(name="get_agents")
def get_agents(data: dict):
    aggregator = Aggregator.parse_obj(data)
    cor = aggregator.init()
    run_async(run(cor))


@app.task(name='get_reports')
def get_report(agg: dict, data: dict):
    aggregator = Aggregator.parse_obj(agg)
    data['agents'] = [Agent.parse_obj(obj) for obj in data['agents']] if data['agents'] else None
    data['start_date'] = datetime.datetime.strptime(data['start_date'].split("T")[0], "%Y-%m-%d")
    data['end_date'] = datetime.datetime.strptime(data['end_date'].split("T")[0], "%Y-%m-%d")
    coro = generate_report(aggregator=aggregator, **data)
    run_async(run(coro))
