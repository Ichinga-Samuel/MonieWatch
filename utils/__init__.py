from functools import partial, wraps

from pydantic import BaseModel


class ResponseModel(BaseModel):
    message: str
    status: bool = True
    data: dict = {}


def error_handler(func=None, error=None):
    if func is None:
        return partial(error_handler, error=error)

    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as err:
            print(err)
            err = error or "something went wrong"
            return ResponseModel(message=err, status=False)

    return wrapper


response = ResponseModel(message="")
