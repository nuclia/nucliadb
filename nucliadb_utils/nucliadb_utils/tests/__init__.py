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
import random
import socket


def free_port(retries=20) -> int:
    port_range_start = 1024  # Start of the port range (1024 is usually safe)
    port_range_end = 65535  # End of the port range

    for _ in range(retries):
        port = random.randint(port_range_start, port_range_end)
        try:
            sock = socket.socket()
            sock.bind(("", port))
            sock.close()
            return port
        except OSError:
            continue

    # give up and let OS pick a free port
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port
