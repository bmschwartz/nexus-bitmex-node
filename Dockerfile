# syntax = docker/dockerfile:1.0-experimental

FROM python:3.8-slim-buster

ENV AWS_DEFAULT_REGION=us-east-1
ENV APP_ENV=development

WORKDIR /app
COPY . /app

RUN --mount=type=secret,id=netrc,target=/root/.netrc pip install -r requirements.txt


CMD ["python", "-m", "nexus_bitmex_node", "start"]