FROM python:3.10-slim-buster

WORKDIR /user/moniewatch

COPY ./requirements.txt .

RUN pip install -U pip

RUN pip install --no-deps -r requirements.txt

COPY . .