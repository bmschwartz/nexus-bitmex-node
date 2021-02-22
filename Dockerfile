# syntax = docker/dockerfile:1.0-experimental

FROM python:3.8-slim-buster

ENV APP_ENV=development

WORKDIR /app
COPY . /app

RUN --mount=type=secret,id=netrc,target=/root/.netrc pip install -r requirements.txt

RUN nohup python -u -m nexus_bitmex_node start --port 8001
RUN nohup python -u -m nexus_bitmex_node start --port 8002
RUN nohup python -u -m nexus_bitmex_node start --port 8003
RUN nohup python -u -m nexus_bitmex_node start --port 8004
RUN nohup python -u -m nexus_bitmex_node start --port 8005

CMD ["ps", "aux", "|", "grep", "python"]