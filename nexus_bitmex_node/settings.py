import os
import time
from enum import Enum
import logging

from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings

from nexus_bitmex_node.ssm_parameter_store import SSMParameterStore
from nexus_bitmex_node.logger import LogLevel, LoggingFormat, generate_logging_config


class ServerMode(Enum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"
    STAGING = "staging"
    DEMO = "demo"


def serialize_log_level(level: LogLevel) -> str:
    return level.value.upper()


config = Config(".env")

logging.basicConfig(level=logging.INFO)

app_env = os.environ["APP_ENV"]
SETTINGS = SSMParameterStore(prefix=f"/nexus/{app_env}")

# Static Settings

APP_NAME = "nexus_bitmex_node"

# Development Settings

SERVER_RELOAD = config("SERVER_RELOAD", cast=bool, default=False)

# Production Settings

HOST = config("HOST", default="127.0.0.1")
PORT = config("PORT", cast=int, default=8081)

# Redis
REDIS_URL = SETTINGS['redis']['REDIS_URL']
print("REDIS_URL")
time.sleep(0.5)

# RabbitMQ
AMQP_URL = SETTINGS['rabbitmq']['AMQP_URL']
print("AMQP_URL")
time.sleep(0.5)

BITMEX_EXCHANGE = SETTINGS['rabbitmq']['BITMEX_EXCHANGE']
print("BITMEX_EXCHANGE")
time.sleep(0.5)

# Logging Configuration

LOG_LEVEL_ENUM = config("LOG_LEVEL", cast=LogLevel, default=LogLevel.INFO)
LOG_LEVEL = LOG_LEVEL_ENUM.value
IS_DEBUG = LOG_LEVEL_ENUM == LogLevel.DEBUG
DEBUG_LOGGERS = config("DEBUG_LOGGERS", cast=CommaSeparatedStrings, default=[])
WARNING_LOGGERS = config("WARNING_LOGGERS", cast=CommaSeparatedStrings, default=[])
INFO_LOGGERS = config("INFO_LOGGERS", cast=CommaSeparatedStrings, default=[])
SERVER_MODE = config("SERVER_MODE", cast=ServerMode, default=ServerMode.DEV)
LOGGING_FORMAT = config(
    "LOGGING_FORMAT", cast=LoggingFormat, default=LoggingFormat.DEFAULT
)

if SERVER_MODE not in [ServerMode.STAGING, ServerMode.DEMO, ServerMode.PROD]:
    LOGGING_CONFIG = generate_logging_config(
        APP_NAME,
        level=serialize_log_level(LOG_LEVEL_ENUM),
        log_handler="logging.StreamHandler",
        formatter=LOGGING_FORMAT.value,
        debug_loggers=DEBUG_LOGGERS,
        warning_loggers=WARNING_LOGGERS,
        info_loggers=INFO_LOGGERS,
    )
else:
    LOGGING_CONFIG = generate_logging_config(
        APP_NAME,
        level=serialize_log_level(LOG_LEVEL_ENUM),
        debug_loggers=DEBUG_LOGGERS,
        warning_loggers=WARNING_LOGGERS,
        info_loggers=INFO_LOGGERS,
    )

# dictConfig(LOGGING_CONFIG)
