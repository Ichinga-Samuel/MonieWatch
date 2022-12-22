from fastapi import APIRouter, Depends

from .dependencies import create_report, check_task
from utils import ResponseModel, error_handler

router = APIRouter(prefix="/api/v1/report")


@router.post('/')
@error_handler
async def report(res: ResponseModel = Depends(create_report)):
    return res


@router.get('/task/{task_id}')
@error_handler
async def task(res: ResponseModel = Depends(check_task)):
    return res

