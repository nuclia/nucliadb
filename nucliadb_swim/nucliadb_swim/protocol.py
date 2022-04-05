import asyncio
import socket
from asyncio import BaseTransport, DatagramProtocol, DatagramTransport, Queue
from typing import Optional, Tuple, cast

import aiodns  # type: ignore

from nucliadb_swim import logger


class SocketAddr(str):
    @property
    def host(self):
        splitted = self.split(":")
        host = ":".join(splitted[:-1])
        return host

    @property
    def port(self):
        splitted = self.split(":")
        return int(splitted[-1])


class SwimProtocol(DatagramProtocol):
    def __init__(self) -> None:
        super().__init__()
        self._transport: Optional[DatagramTransport] = None
        self.queue: Queue[Tuple[bytes, SocketAddr]] = Queue()
        self._resolver = None

    @property
    def resolver(self):
        if self._resolver is not None:
            return self._resolver
        loop = asyncio.get_event_loop()
        self._resolver = aiodns.DNSResolver(loop=loop)
        return self._resolver

    @property
    def transport(self) -> DatagramTransport:
        """The current :class:`~asyncio.DatagramTransport` object."""
        transport = self._transport
        assert transport is not None
        return transport

    def connection_made(self, transport: BaseTransport) -> None:
        """Called when the UDP socket is initialized.

        See Also:
            :meth:`asyncio.BaseProtocol.connection_made`

        """
        self._transport = cast(DatagramTransport, transport)

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:

        socket_addr = SocketAddr(f"{addr[0]}:{addr[1]}")

        self.queue.put_nowait((data, socket_addr))

    def error_received(self, exc: Exception) -> None:
        """Called when a UDP send or receive operation fails.

        See Also:
            :meth:`asyncio.DatagramProtocol.error_received`

        """
        logger.exception("UDP operation failed")

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Called when the UDP socket is closed.

        See Also:
            :meth:`asyncio.BaseProtocol.connection_lost`

        """
        logger.error("UDP connection lost: %s", exc)
        self._transport = None

    def recv(self) -> Tuple[bytes, SocketAddr]:
        return self.queue.get_nowait()

    async def send(self, addr: SocketAddr, packet: bytes) -> None:
        try:
            self.transport.sendto(packet, (addr.host, addr.port))
        except AttributeError:
            addr = SocketAddr(addr)
            self.transport.sendto(packet, (addr.host, addr.port))
        except ValueError:
            new_addr = await self.resolver.gethostbyname(addr.host, socket.AF_INET)
            if len(new_addr.addresses) > 0:
                self.transport.sendto(packet, (new_addr.addresses[0], addr.port))
            else:
                raise Exception(f"Could not find on DNS the name {addr.host}")
