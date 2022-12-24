from logging import getLogger
from datetime import datetime, timedelta, date

from fastapi import HTTPException, Depends, status, Body, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, Field
from jose import JWTError, jwt
from tortoise.transactions import in_transaction
from tortoise.exceptions import IntegrityError
from celery.result import AsyncResult

from models.aggregator import CreateAggregator, Aggregator, AggregatorORM, Agent, AgentORM, Report
from utils.env import env
from utils import error_handler, ResponseModel
from utils.worker import get_agents, get_report

logger = getLogger()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/v1/auth/login")


class Token(BaseModel):
    access_token: str = Field(..., alias="accessToken")
    token_type: str = Field('bearer', alias="tokenType")


@error_handler(error="Something Went Wrong")
async def create(aggregator: CreateAggregator) -> ResponseModel:
    async with in_transaction():
        try:
            agg = await AggregatorORM.create(**aggregator.dict())
            agg = Aggregator(email=agg.email, username=agg.username, password=agg.password)
        except IntegrityError as err:
            logger.error(err)
            return ResponseModel(message="A user with this username already exists", status=False)
        except Exception as err:
            logger.error(err)
            raise err
        else:
            data = agg.dict()
            get_agents.delay(data)
            return ResponseModel(message="User Created Successfully")


def create_access_token(data: dict) -> str:
    data.update({'exp': datetime.utcnow() + timedelta(hours=24)})
    return jwt.encode(data, env.SECRET_KEY, env.ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, env.SECRET_KEY, algorithms=[env.ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Token", headers={"WWW-Authenticate": "Bearer"})


async def get_aggregator_from_token(token: str = Depends(oauth2_scheme)) -> AggregatorORM:
    try:
        payload = decode_access_token(token)
        aggregator = await AggregatorORM.get(username=payload.get('sub')).prefetch_related('agents')
        return aggregator
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unable to find user", headers={"WWW-Authenticate": "Bearer"})


@error_handler(error="Unable to get user data")
async def get_aggregator(aggregator: AggregatorORM = Depends(get_aggregator_from_token)) -> ResponseModel:
    agents = [Agent.from_orm(agent) for agent in aggregator.agents]
    aggregator: Aggregator = Aggregator.from_orm(aggregator)
    data = aggregator.dict(exclude_none=True)
    data['agents'] = agents
    return ResponseModel(message="Successful", data=data)


@error_handler(error="Incorrect Password or Username")
async def authenticate(login: OAuth2PasswordRequestForm = Depends()) -> ResponseModel:
    user = await AggregatorORM.get(username=login.username).prefetch_related('agents', 'reports')
    agg = Aggregator(username=user.username, password=user.password, email=user.email)
    reports = [Report(**obj).dict(by_alias=True) for obj in await user.reports.all().values()]
    agents = [Agent.from_orm(obj).dict(by_alias=True) for obj in user.agents]
    data = {'password': user.password, 'sub': user.username}
    token = create_access_token(data)
    return ResponseModel(message="Login Successful", data={'token': token, **agg.dict(exclude_none=True, exclude={'password'}), "agents": agents,
                                                           'reports': reports})


@error_handler(error="Unable to Process Report Try Again")
async def create_report(target: float = Body(), agents: list[dict] = Body(), start: date = Body(), end: date = Body(),
                          aggregator: Aggregator = Depends(get_aggregator_from_token)):
    agg = Aggregator(email=aggregator.email, password=aggregator.password, username=aggregator.username, name=aggregator.name)
    agg = agg.dict()
    data = {'target': target, 'start_date': start, 'end_date': end, 'agents': agents}
    task_id = get_report.delay(agg, data)
    return ResponseModel(message="Your Report Will be Available Shortly", data={'taskId': str(task_id)})


@error_handler(error="Something Went Wrong")
async def check_task(task_id: str):
    task = AsyncResult(task_id)
    data = {'task': task.result, 'status': task.status, 'taskId': task.id}
    return ResponseModel(message=f"Task: {task_id}", data=data)
