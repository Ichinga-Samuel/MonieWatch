from utils.cloud_upload import S3
import asyncio


async def test_storage():
    s3 = S3()
    file = open('../requirements.txt', 'rb')
    res = await s3(file=file)
    print(res.response.public_url)


if __name__ == "__main__":
    asyncio.run(test_storage())
