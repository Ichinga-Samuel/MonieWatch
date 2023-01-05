import datetime
from functools import cache
from typing import Optional, List, Any
import logging

from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr, Field, validator, AnyUrl

from utils.client import ClientTransaction, Auth, Agent
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
    def session(self):
        return ClientTransaction(auth=Auth(username=self.username, password=self.password))

    @property
    async def agents(self):
        agg = await self.orm
        await agg.fetch_related('agents')
        return [Agent.from_orm(obj) for obj in agg.agents]

    async def init(self):
        try:
            await self.authenticate()
            await self.update_agents()
            await self.profile_update()
            await self.session.close()
        except Exception as ex:
            logging.error(ex)
            await self.session.close()

    async def update(self, **kwargs):
        orm = await self.orm
        await orm.update_from_dict(kwargs)
        await orm.save(update_fields=tuple(kwargs.keys()))

    async def profile_update(self):
        profile = await self.session.profile()
        await self.update(**profile.dict)

    async def authenticate(self):
        await self.session.authenticate()
        return self.session.is_auth

    def verify_password(plain_password: str, hashed_password: str) -> bool:
        return pwd_context.verify(plain_password, hashed_password)

    async def update_agents(self):
        sess = self.session
        api_agents = {Agent(**agent.dict()) for agent in await sess.get_agents()}
        db_agents = await self.agents
        new_agents = api_agents - {*db_agents}
        aggregator = await self.orm
        await AgentORM.bulk_create(objects=[AgentORM(**agent.dict(), aggregator=aggregator) for agent in new_agents])

    async def get_transactions(self, start_date: datetime.date | None = None, end_date: datetime.date | None = None, target: None | float = None,
                               agents: List[Agent] | None = None, title: str = ""):
        today = datetime.date.today()
        start_date = start_date or today
        end_date = end_date or today
        title = title or f"Transactions Report for {self.name} {start_date.strftime('%A, %B %d %Y')}"
        session = self.session
        agents = agents or await self.agents
        transactions = await session.get_transactions_by_agents(start_date=start_date, end_date=end_date, agents=agents)
        trans = Transactions(title=title, transactions=transactions, agents=agents)
        trans.target = target or trans.target
        pdf = await trans.get_pdf()
        if not pdf:
            logger.error("Unable to generate pdf report")
            return False
        s3 = S3(extra_args={"ACL": "public-read"})
        s3 = await s3(file=pdf)
        if s3.response.status:
            agg = await self.orm
            rep = await ReportORM.create(name=pdf.name.rsplit('.')[-2], url=s3.response.public_url, aggregator=agg)
            mail = ReportEmail(name=self.name, link=rep.url, recipients=[self.email])
            await mail.send()
        await session.close()
        return True


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

    async def create(self) -> bool:
        try:
            await AggregatorORM.create(**self.dict())
            return True
        except Exception as err:
            return False


class Report(BaseModel):
    name: str
    url: AnyUrl
    date: str

    def __init__(self, **kwargs):
        kwargs['date'] = kwargs['date'].isoformat()
        super(Report, self).__init__(**kwargs)

    class Config:
        orm_mode = True
