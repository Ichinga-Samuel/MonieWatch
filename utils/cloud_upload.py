from abc import ABC, abstractmethod
import os
import asyncio
import logging
from urllib.parse import quote as urlencode
from typing import BinaryIO
from pathlib import Path
from io import BufferedReader

from pydantic import BaseModel, HttpUrl
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from .env import env

logger = logging.getLogger(__name__)


class FileData(BaseModel):
    public_url: HttpUrl | str = ""
    status: bool = True


class CloudUpload(ABC):

    def __init__(self, *, file: Path | BinaryIO | None = None, files: list[Path | BinaryIO] | None = None, name: str = "",
                 extra_args: dict | None = None):

        self.file = file
        self.files = files
        self.response: FileData | list[FileData] = FileData()
        self.extra_args = extra_args or {}
        self.name = name

    async def __call__(self, file: Path | BinaryIO | None = None, files: list[BufferedReader | bytes] | None = None):
        if file:
            self.file = file
            await self.upload()

        elif files:
            self.files = files
            await self.multi_upload()
        return self

    @abstractmethod
    async def upload(self, name: str = "") -> FileData:
        """"""

    @abstractmethod
    async def multi_upload(self, *args, **kwargs) -> list[FileData]:
        """"""


class S3(CloudUpload):

    def __init__(self, *, file: BinaryIO | Path | None = None, files: list[BufferedReader | Path] | None = None, region_name: str = "",
                 aws_access_key_id: str = "", aws_secret_access_key: str = "", bucket_name: str = "", name: str = "", extra_args: dict | None = None):

        super().__init__(file=file, files=files, name=name, extra_args=extra_args)
        self.region_name = region_name or env.AWS_REGION
        self.aws_access_key_id = aws_access_key_id or env.AWS_ACCESS_KEY
        self.aws_secret_access_key = aws_secret_access_key or env.AWS_SECRET_KEY
        self.bucket_name = bucket_name or env.S3_BUCKET_NAME

    async def get_client(self):
        return await asyncio.to_thread(boto3.client, 's3', region_name=self.region_name, aws_access_key_id=self.aws_access_key_id,
                                       aws_secret_access_key=self.aws_secret_access_key)

    async def upload(self, name: str = ""):
        """"""
        name = name or self.name
        file_data = await self._upload_file(file=self.file, name=name)
        self.response = file_data
        return self.response

    async def _upload_file(self, *, file: Path | BinaryIO, name: str = "", client=None) -> FileData:
        try:
            s3 = client or await self.get_client()
            object_name, object_ = (name or file.name, file.open(mode='rb')) if isinstance(file, Path) else (name or file.name.rsplit('/')[-1], file)
            await asyncio.to_thread(s3.upload_fileobj, object_, self.bucket_name, object_name, ExtraArgs=self.extra_args)
            url = f"https://{self.bucket_name}.s3.{self.region_name}.amazonaws.com/{urlencode(object_name.encode('utf8'))}"
            return FileData(public_url=url)
        except (NoCredentialsError, ClientError, Exception) as err:
            logger.error(err)
            return FileData(status=False)

    async def multi_upload(self, *args, **kwargs):
        client = await self.get_client()
        tasks = [asyncio.create_task(self._upload_file(file=file, client=client)) for file in self.files]
        self.response = await asyncio.gather(*tasks)
        return self.response
