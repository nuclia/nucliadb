# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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
