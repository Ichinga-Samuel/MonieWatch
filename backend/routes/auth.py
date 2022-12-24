from fastapi import APIRouter, Depends

from .dependencies import authenticate, create, get_aggregator
from models.aggregator import Aggregator
from utils import ResponseModel, error_handler

router = APIRouter(prefix='/api/v1/auth')


@router.post('/signup')
@error_handler(error="Unable to create user")
async def signup(res: ResponseModel = Depends(create)):
    return res


@router.post('/login')
@error_handler
async def login(res: ResponseModel = Depends(authenticate)):
    return res


@router.get('/get')
@error_handler(error="Unsuccessful")
async def get_aggregator(res: ResponseModel = Depends(get_aggregator)):
    return res
