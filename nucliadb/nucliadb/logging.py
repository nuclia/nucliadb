# this is default (site-packages\uvicorn\main.py)
log_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(asctime)s %(levelprefix)s %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO", "handlers": ["default"], "propagate": False},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        "nucliadb": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb.one": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb.ingest": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": False,
        },
        "nucliadb.search": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb.writer": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb.reader": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
    },
}
