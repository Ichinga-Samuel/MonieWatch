import asyncio
import random
import datetime
from typing import Iterable
from logging import getLogger

from httpx import AsyncClient

from .env import env
from .data_models import Agent, Auth, Transaction, Profile


logger = getLogger()


class ClientTransaction:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.headers = {"authority": env.AUTHORITY, "origin": env.ORIGIN, "referer": env.REFERER, "client-type": "WEB", "client-version": "0.0.0"}
        self.params = {"pageNumber": 1, "pageSize": 1000}
        self.url = env.API_URL
        self.client = AsyncClient(base_url=self.url, headers=self.headers)
        self.maximum_backoff = 20
        self.cache = {}

    @property
    def is_auth(self):
        return self.auth.status

    async def authenticate(self, retries=3, backoff=1):
        try:
            data = {"username": self.auth.username, "password": self.auth.password, "secret": self.auth.password,
                    **self.device}
            res = await self.client.post("/auth/tokens", json=data)
            res = res.json()
            token = res['tokenData']['access_token']
            self.client.headers['Authorization'] = self.headers['Authorization'] = f"Bearer {token}"
            self.auth.status = True
            self.auth.token = f"Bearer {token}"
            return True
        except Exception as err:
            if retries > 0:
                await asyncio.sleep(self.get_backoff(backoff=backoff))
                await self.authenticate(retries=retries-1, backoff=backoff+1)
            print(err, "Unable to authenticate")
            self.auth.status = False
            return False

    @property
    def device(self) -> dict:
        dui = random.randint(68000000, 70000000)
        return {
            "deviceIdentifier": dui,
            "manufacturer": random.choice(("Apple", "Windows", "Linux")),
            "model": random.choice(("Chrome", "Edge", "Opera", "Safari", "Mozilla")),
            "deviceUniqueIdentifier": dui,
        }

    def get_backoff(self, backoff):
        return min(self.maximum_backoff, backoff**2 + random.randint(100, 1000))

    async def profile(self):
        res = await self.get_json(url="/profiles/aggregators")
        profile = res['profile']
        name = profile.get("firstName", "").title() + " " + profile.get("lastName", "").title()
        mobile = int(profile.get('mobileNumber', 0))
        profile = Profile(name=name, mobile=mobile)
        return profile

    async def get_consolidated_transactions(self, *, start_date: datetime.date, end_date: datetime.date, agent_id: int = 0) -> list[Transaction]:
        start_date, end_date = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        params = {**self.params, "startDate": start_date, "endDate": end_date, "amount": 0, "terminalId": 0, "hardwareTerminalId": 0,
                  "agentId": agent_id or "", "status": "COMPLETED", "reference": ""}
        url = "/aggregators/consolidated-transactions/"
        res = await self.get_json(url=url, params=params)
        if not res:
            return []
        pages = res.get('totalPages', 1)
        transactions = [Transaction.create(trans) for trans in res["consolidatedTransactions"] if trans['status'] == "COMPLETED" and
                        not trans['reversed'] and not trans['shouldBeReversed']]

        async def get_transactions(page_number):
            nonlocal params
            params = {**params, "pageNumber": page_number}
            resp = await self.get_json(url=url, params=params)
            if not resp or (resp.get("consolidatedTransactions") is not None or []):
                return []
            transactions.extend(Transaction.create(trans) for trans in resp["consolidatedTransactions"] if trans['status'] == "COMPLETED" and
                                not trans['reversed'] and not trans['shouldBeReversed'])

        tasks = [get_transactions(i) for i in range(2, pages + 1)]
        await asyncio.gather(*tasks)
        return transactions

    async def get_agents(self) -> list[Agent]:
        try:
            params = {**self.params, "keyword": '', "status": ''}
            url = "/agents"
            data = await self.get_json(url=url, params=params)
            if not data:
                return []
            pages = data.get('totalPages', 1)
            agents = data.get('agents', [])
            agents = [Agent(agent_id=agent['id'], name=agent['businessName'].title(), mobile=agent['mobileNumber']) for agent in data['agents']]

            async def other_pages(page_number):
                nonlocal params
                params = {**params, "pageNumber": page_number}
                resp = await self.get_json(url=url, params=params)
                if not resp or resp.get('agents'):
                    return []
                agents.extend(Agent(agent_id=agent['id'], name=agent['businessName'], mobile=agent['mobileNumber']) for agent in resp['agents'])

            if pages > 1:
                tasks = [other_pages(i) for i in range(1, pages + 1)]
                await asyncio.gather(*tasks)
            return agents
        except Exception as err:
            print(err, 'Unable to get agents')

    async def get_transactions_by_agents(self, start_date: datetime.date, end_date: datetime.date, agents: list[Agent] | None = None) -> Iterable[
        Transaction]:
        agents = agents or await self.get_agents()
        agent_transactions = []
        for agent in agents:
            transactions = await self.get_consolidated_transactions(start_date=start_date, end_date=end_date, agent_id=agent.agent_id)
            agent_transactions.extend(transactions)
        return (transaction for transaction in agent_transactions)

    async def get_json(self, *, url: str, retry=True, retries=3, backoff=1, **kwargs):
        try:
            res = await self.client.get(url, **kwargs)
            data = res.json()
            error = data.get('error')
            if error == "invalid_token" or error == 'Unauthorized' and retry:
                await self.authenticate()
                return await self.get_json(url=url, retry=False, **kwargs)

            if data.get("responseCode") == "20000":
                return data

            else:
                if retries > 0:
                    await asyncio.sleep(self.get_backoff(backoff))
                    return await self.get_json(url=url, retry=False, retries=retries - 1, backoff=backoff+1, **kwargs)
            return {}

        except Exception as err:
            if retries > 0:
                await asyncio.sleep(self.get_backoff(backoff))
                return await self.get_json(url=url, retry=False, retries=retries - 1, backoff=backoff+1, **kwargs)
            return {}

    async def close(self):
        await self.client.aclose()

