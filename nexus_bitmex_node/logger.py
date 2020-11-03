import json
import logging
import traceback
import pytz
from pytz import utc
from enum import Enum
from typing import Optional, List, Dict, Any
from collections import OrderedDict
from datetime import datetime
from logging import getLogger
from dateutil.parser import parse
from dateutil.utils import default_tzinfo

_IGNORED_LOGS = [
    '"GET /healthz HTTP/1.0" 200',
    '"GET /healthz HTTP/1.1" 200',
    '"GET /metrics HTTP/1.0" 200',
    '"GET /metrics HTTP/1.1" 200',
]


logger = getLogger(__name__)


class LogLevel(Enum):
    INFO = "info"
    DEBUG = "debug"
    WARNING = "warning"
    CRITICAL = "critical"


class LoggingFormat(Enum):
    DEFAULT = "default"
    DETAILED = "detailed"
    VERBOSE = "verbose"


def to_str(key, val, **kwargs):
    return {key: str(val)}


def to_isodate(key, val, **kwargs):
    if isinstance(val, str):
        try:
            date = parse(val)
        except ValueError | OverflowError:
            logger.exception(f"Normalization error: Failed to parse {val} for {key}")
            return {}
    elif isinstance(val, datetime):
        date = val
    else:
        logger.error(
            f"Normalization error: Expected {key} to be a datetime instead received {val}"
        )
        return {}
    return default_tzinfo(date, utc).isoformat(timespec="microseconds")


def to_int(key, val, **kwargs):
    try:
        return {key: int(val)}
    except ValueError:
        logger.error(f"Normalization error: Could not cast {val} to int for {key}")
        return {}


def to_float(key, val, **kwargs):
    try:
        return {key: float(val)}
    except ValueError:
        logger.error(f"Normalization error: Could not cast {val} to float for {key}")
        return {}


def to_bool(key, val, **kwargs):
    if isinstance(val, bool):
        return {key: val}
    return {key: bool(val)}


def to_list(key, val, **kwargs):
    if isinstance(val, list):
        return {key: val}
    return {key: list(val)}


def to_dict(key, val, normalization_map=None):
    if isinstance(val, dict):
        return {key: sanitize_insert_record(val, normalization_map=normalization_map)}
    elif isinstance(val, list):
        return {key: {"value_normalized_to_list": val}}
    return {key: {"value": str(val)}}


def omit(key, val, **kwargs):
    return {}


def sanitize_insert_record(body, normalization_map=None):
    normalization_map = normalization_map or {}
    for key in list(body.keys()):
        val = body[key]
        if val is None:
            del body[key]
        elif key in normalization_map.keys():
            del body[key]
            normalize = normalization_map[key]
            if callable(normalize):
                body.update(normalize(key, val, normalization_map=normalization_map))
            else:
                logger.error(
                    f'Failed to santize {key}. normalization_map "{normalize}" is not callable. Removed {key} from record'
                )
        elif isinstance(val, list):
            del body[key]
            body[f"{key}_normalized_to_list"] = val
        elif isinstance(val, dict):
            del body[key]
            body[f"{key}_normalized_to_dict"] = sanitize_insert_record(
                val, normalization_map=normalization_map
            )
        else:
            body[key] = str(val)

    return body


class JsonStreamHandler(logging.StreamHandler):
    def filter(self, record: logging.LogRecord) -> bool:
        if not super(JsonStreamHandler, self).filter(record):
            return False

        if record.msg:
            for ignored in _IGNORED_LOGS:
                if ignored in record.msg:
                    return False

        return True

    def format(self, record: logging.LogRecord) -> str:

        time = datetime.now(tz=pytz.utc).isoformat(timespec="microseconds")
        log_message = OrderedDict(
            [
                ("level", record.levelname),
                ("process", record.process),
                ("channel", record.name),
                ("function_name", record.funcName),
            ]
        )

        if getattr(record, "exc_info", None):
            exc_type, exc_value, exc_traceback = record.exc_info  # type: ignore
            log_message["exception"] = traceback.format_exception(
                exc_type, exc_value, exc_traceback
            )

        if getattr(record, "response_status", None):
            log_message["status"] = record.response_status  # type: ignore

        if getattr(record, "response_size", None):
            log_message["response_size"] = record.response_size  # type: ignore

        try:
            message = json.loads(
                record.msg if isinstance(record.msg, str) else json.dumps(record.msg)
            )
            if isinstance(message, dict):
                if "message" in message:
                    log_message["message"] = message["message"]
                    del message["message"]
                log_message["metadata"] = message
            elif isinstance(message, list):
                log_message["metadata"] = message
            else:
                message["message"] = message
        except:
            log_message["message"] = record.getMessage()

        log_message["time"] = time

        if "source" in log_message:
            log_message["source_remapped"] = log_message["source"]
            del log_message["source"]

        try:
            return json.dumps(
                sanitize_insert_record(
                    log_message,
                    normalization_map={
                        "traceback": to_list,
                        "exception": to_list,
                        "message": to_str,
                        "metadata": to_dict,
                        "row_number": to_int,
                        "ordered_at": to_isodate,
                        "callback_url": omit,
                    },
                )
            )
        except TypeError:
            return f"{log_message}"


def generate_logging_config(
    name: str,
    level: Optional[str] = "DEBUG",
    log_handler: Optional[str] = None,
    formatter: Optional[str] = "detailed",
    debug_loggers: List[str] = [],
    warning_loggers: List[str] = [],
    info_loggers: List[str] = [],
) -> Dict[str, Any]:
    console_handler = log_handler if log_handler else f"{__name__}.JsonStreamHandler"

    config: Dict[str, Any] = {
        "version": 1,
        "formatters": {
            "default": {"format": "%(levelname)-7s [%(name)s:%(module)s] %(message)s"},
            "detailed": {
                "format": "%(levelname)-7s %(asctime)s [%(name)s:%(module)s] %(message)s",
            },
            "verbose": {
                "format": "%(levelname)-7s %(asctime)s [%(name)s:%(module)s] [%(process)d:%(thread)d] %(message)s"
            },
        },
        "handlers": {
            "console": {
                "class": console_handler,
                "formatter": formatter,
            },
        },
        "loggers": {
            "root": {"handlers": ["console"], "level": "INFO"},
            "asyncio": {"handlers": ["console"], "level": "WARNING"},
            "uvicorn.error": {"handlers": ["console"], "level": "INFO"},
            "service_health_checks": {"handlers": ["console"], "level": level},
        },
    }

    for logger in debug_loggers:
        config["loggers"][logger] = {"handlers": ["console"], "level": "DEBUG"}

    for logger in warning_loggers:
        config["loggers"][logger] = {"handlers": ["console"], "level": "WARNING"}

    for logger in info_loggers:
        config["loggers"][logger] = {"handlers": ["console"], "level": "INFO"}

    return config
