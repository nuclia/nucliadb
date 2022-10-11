# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
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
