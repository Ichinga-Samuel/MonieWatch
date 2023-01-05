import asyncio
from json import JSONDecodeError
import random
import datetime
from typing import Iterable
from logging import getLogger

from httpx import AsyncClient, RequestError, Headers

from .env import env
from .task_queue import Worker
from .data_models import Agent, Auth, Transaction, Profile

logger = getLogger()


class ClientTransaction:
    def __init__(self, auth: Auth):
        self.auth = auth
        self.headers = Headers(
            {"authority": env.AUTHORITY, "origin": env.ORIGIN, "referer": env.REFERER, "client-type": "WEB", "client-version": "0.0.0"})
        self.params = {"pageNumber": 1, "pageSize": 1000}
        self.url = env.API_URL
        self.client = AsyncClient(base_url=self.url, headers=self.headers)

    @property
    def is_auth(self):
        return self.auth.status

    async def authenticate(self, trie=0):
        try:
            data = {"username": self.auth.username, "password": self.auth.password, "secret": self.auth.password,
                    **self.device}
            res = await self.client.post("/auth/tokens", json=data)
            res = res.json()

            if not isinstance(res, dict):
                raise TypeError("Invalid Response")

            if res.get('responseCode') == 'invalid_grant':
                logger.warning("Wrong Password")
                return False

            token = res['tokenData']['access_token']
            self.client.headers['Authorization'] = self.headers['Authorization'] = f"Bearer {token}"
            self.auth.status = True
            self.auth.token = f"Bearer {token}"
            return True
        except (RequestError, TypeError, JSONDecodeError) as err:
            if trie > 2:
                await asyncio.sleep(self.backoff(trie=trie))
                trie = trie + 1
                await self.authenticate(trie=trie)

            logger.error(err)
            return False

        except Exception as err:
            logger.error(err)
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

    @staticmethod
    def backoff(self, trie=0):
        return min(64, 2 ** trie + (random.randint(1, 1000)) / 1000)

    async def profile(self):
        try:
            res = await self.get_json(url="/profiles/aggregators")
            profile = res['profile']
            name = profile.get("firstName", "").title() + " " + profile.get("lastName", "").title()
            mobile = int(profile.get('mobileNumber', 0))
            profile = Profile(name=name, mobile=mobile)
            return profile
        except Exception as err:
            logger.warning(err)

    async def get_consolidated_transactions(self, *, start_date: datetime.date, end_date: datetime.date, agent_id: int = 0)\
            -> list[Transaction] | None:
        try:
            params = {**self.params, "startDate": start_date.strftime("%Y-%m-%d"), "endDate": end_date.strftime("%Y-%m-%d"), "amount": 0,
                      "terminalId": 0, "hardwareTerminalId": 0, "agentId": agent_id or "", "status": "COMPLETED", "reference": ""}
            url = "/aggregators/consolidated-transactions/"
            res = await self.get_json(url=url, params=params)
            consolidated_transactions = res.get("consolidatedTransactions", [])
            transactions = [Transaction.create(trans) for trans in consolidated_transactions if trans['status'] == "COMPLETED" and
                            not trans['reversed'] and not trans['shouldBeReversed']]

            if pages := res.get('otherPages', []):
                for page in pages:
                    trans = page.get("consolidatedTransactions", [])
                    transactions.extend(Transaction.create(tran) for tran in trans if tran['status'] == "COMPLETED" and not tran['reversed'] and not
                    tran['shouldBeReversed'])

            return transactions
        except Exception as err:
            logger.warning(err)
            return

    async def get_agents(self) -> list[Agent] | None:
        try:
            params = {**self.params, "keyword": '', "status": ''}
            url = "/agents"
            data = await self.get_json(url=url, params=params)
            agents = data['agents']

            if pages := data.get('otherPages', []):
                for page in pages:
                    ags = page.get('agents', [])
                    agents.extend(ags)

            return [Agent(agent_id=agent['id'], name=agent['businessName'].title(), mobile=agent['mobileNumber']) for agent in agents]
        except Exception as err:
            logger.warning(err)

    async def get_transactions_by_agents(self, start_date: datetime.date, end_date: datetime.date, agents: list[Agent] | None = None) -> Iterable[Transaction]:
        try:
            agents = agents or await self.get_agents()
            agent_transactions = []
            args = [{'start_date': start_date, 'end_date': end_date, 'agent_id': agent.agent_id} for agent in agents]
            worker = Worker(self.get_consolidated_transactions, args=args, workers=len(agents))
            await worker.run()
            [agent_transactions.extend(trans) for trans in worker.responses if trans]
            return agent_transactions
        except Exception as exe:
            logger.warning(exe)

    async def get_json(self, *, url: str, trie=0, paginate=True, **kwargs):
        try:
            res = await self.client.get(url, **kwargs)

            if res.status_code != 200:
                raise ValueError("Unsuccessful Request")

            res = res.json()
            if not isinstance(res, dict):
                raise TypeError("Invalid Response")

            if res.get("responseCode") == "20000":

                if (pages := res['totalPages']) > 1 and paginate:
                    worker = Worker(self.get_json, workers=pages)
                    arg = {'url': url, 'trie': 0, 'paginate': False}
                    for i in range(2, pages + 1):
                        kwargs['params']['pageNumber'] = i
                        arg |= kwargs
                        worker.add(arg)
                        await worker.run()
                    res['otherPages'] = [page for page in worker.responses if page is not None]

                return res

        except (TypeError, RequestError) as err:
            if trie > 2:
                await asyncio.sleep(self.backoff(trie=trie))
                trie = trie + 1
                return await self.get_json(url=url, trie=trie, **kwargs)

            logger.error(err)

        except Exception as err:
            logger.error(err)

    async def close(self):
        await self.client.aclose()
