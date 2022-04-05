from dataclasses import dataclass

from nucliadb_swim.protocol import SocketAddr

CONST_PACKET_SIZE = 1 << 16


@dataclass
class ArtilleryClusterConfig:
    listen_addr: SocketAddr
    cluster_key: bytes = b"default"
    ping_interval: int = 5
    network_mtu: int = CONST_PACKET_SIZE
    ping_request_host_count: int = 3
    ping_timeout: int = 3
