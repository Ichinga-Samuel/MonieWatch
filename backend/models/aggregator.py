import datetime
from typing import Optional, List
from functools import cache
import logging

from tortoise.transactions import in_transaction
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr, Field, validator, AnyUrl

from utils.client import ClientTransaction, Auth, Agent
from utils.data_models import AgentFilter
from utils.cloud_upload import S3
from utils.email import ReportEmail

from .tables_orm import AggregatorORM, AgentORM, ReportORM
from .transaction import Transactions

pwd_context = CryptContext(schemes=['bcrypt'], deprecated="auto")
logger = logging.getLogger()


class Aggregator(BaseModel):
    username: str
    password: str
    email: EmailStr
    mobile: Optional[int]
    reports: Optional[List[AnyUrl]] = []
    name: Optional[str] = ""

    class Config:
        orm_mode = True
        allow_arbitrary_types = True
        extra = 'allow'

    def __hash__(self):
        return hash(self.mobile)

    @property
    async def orm(self) -> type(AggregatorORM):
        orm = await AggregatorORM.get(username=self.username)
        return orm

    @property
    @cache
    def session(self) -> ClientTransaction:
        return ClientTransaction(auth=Auth(username=self.username, password=self.password))

    @property
    async def agents(self):
        agg = await self.orm
        await agg.fetch_related('agents')
        return [Agent.from_orm(obj) for obj in agg.agents]

    async def init(self):
        try:
            async with in_transaction():
                await self.session.authenticate()
                agents = await self.session.get_agents()
                aggregator = await self.orm
                profile = await self.session.profile()
                await AgentORM.bulk_create(objects=[AgentORM(**agent.dict(), aggregator=aggregator) for agent in agents])
                await aggregator.update_from_dict(**profile.dict)
                await aggregator.save(update_fields=tuple(profile.dict.keys()))
                await self.session.close()
                return True
        except Exception as ex:
            logging.critical(ex, "Unable to get agents")
            await self.session.close()

    async def update(self, **kwargs):
        orm = await self.orm
        await orm.update_from_dict(kwargs)
        await orm.save(update_fields=tuple(kwargs.keys()))

    def verify_password(*, plain_password: str, hashed_password: str) -> bool:
        return pwd_context.verify(plain_password, hashed_password)

    async def update_agents(self):
        sess = self.session
        await self.session.authenticate()
        api_agents = {Agent(**agent.dict()) for agent in await sess.get_agents()}
        db_agents = await self.agents
        new_agents = api_agents - {*db_agents}
        aggregator = await self.orm
        await AgentORM.bulk_create(objects=[AgentORM(**agent.dict(), aggregator=aggregator) for agent in new_agents])
        await self.session.close()

    async def get_transactions(self, *, start_date: datetime.date | None = None, end_date: datetime.date | None = None, target: float | None,
                               agents: List[Agent] | None = None, title: str = "") -> Transactions | None:
        try:
            today = datetime.date.today()
            start_date = start_date or today
            end_date = end_date or today
            title = title or f"Transactions Report for {self.name} {start_date.strftime('%A, %B %d %Y')}"
            if await self.session.authenticate():
                transactions = await self.session.get_consolidated_transactions(start_date=start_date, end_date=end_date)
                await self.session.close()
                agents = agents or await self.agents
                filter = AgentFilter(agents=[agent.agent_id for agent in agents])
                transactions = Transactions(title=title, transactions=transactions, agents=agents, filter=filter, target=target or 50000)
                return transactions
        except Exception as err:
            logger.critical(f"{err}: Unable to generate transactions")
            await self.session.close()

    async def get_pdf(self, *, transactions: Transactions):
        return await transactions.get_pdf()

    async def upload_to_cloud(self, *, file) -> dict[str, str]:
        try:
            s3 = S3(extra_args={"ACL": "public-read"})
            s3 = await s3(file=file)
            if s3.response.status:
                name = file.name.rsplit('.')[-2]
                url = s3.response.public_url
                return {'name': name, 'url': url}
        except Exception as err:
            logger.critical(f"{err}: Unable to upload file to cloud")

    async def save_report(self, *, url: str, name: str) -> ReportORM:
        try:
            agg = await self.orm
            rep = await ReportORM.create(name=name, url=url, aggregator=agg)
            return rep
        except Exception as err:
            logger.critical(f"{err}: unable to save report")

    async def send_report(self, *, url):
        try:
            mail = ReportEmail(name=self.name, link=url, recipients=[self.email])
            await mail.send()
        except Exception as err:
            logger.critical(f"{err}: unable to send mail")


class CreateAggregator(BaseModel):
    username: str
    email: EmailStr
    password: str
    confirm_password: str = Field(..., alias="confirmPassword", exclude=True)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @validator('confirm_password')
    def match_password(cls, v, values):
        password = values.get("password")
        if v != password:
            raise ValueError('Password Mismatch')
        return v

    def password_hash(self):
        return pwd_context.hash(self.password)

    class Config:
        allow_population_by_field_name = True
        fields = {'confirm_password': {"exclude": True}}

    @property
    async def orm(self) -> type(AggregatorORM):
        orm = await AggregatorORM.get(username=self.username)
        return orm

    async def create(self) -> AggregatorORM:
        try:
            return await AggregatorORM.create(**self.dict())
        except Exception as err:
            logger.critical(f"{err}: Unable to create aggregator")


class Report(BaseModel):
    name: str
    url: AnyUrl
    date: str

    def __init__(self, **kwargs):
        kwargs['date'] = kwargs['date'].isoformat()
        super(Report, self).__init__(**kwargs)

    class Config:
        orm_mode = True
