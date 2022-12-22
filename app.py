from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from tortoise.contrib.fastapi import register_tortoise

# from app.utils.env import env
from routes.auth import router as auth_router
from routes.report import router as report_router
from utils.db import TORTOISE_ORM
from utils import ResponseModel

app = FastAPI()

register_tortoise(app, config=TORTOISE_ORM, generate_schemas=True, add_exception_handlers=True)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex="http://localhost:[0-9]{4,5}",
    # allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    res = ResponseModel(message=exc.detail, status=False)
    return JSONResponse(content=jsonable_encoder(res), status_code=200)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    print(exc.errors())
    res = ResponseModel(message="Bad input data", data=exc.body, status=False)
    return JSONResponse(status_code=200, content=jsonable_encoder(res))


app.include_router(auth_router)
app.include_router(report_router)
