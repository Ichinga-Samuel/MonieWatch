FROM python:3.10-slim-buster

WORKDIR /user/projects/moniewatch

COPY ./requirements.txt .

RUN pip install -U pip

RUN pip install -r requirements.txt

COPY . .