from nats.errors import NoRespondersError as NoRespondersError
from nats.js import api as api
from nats.js.errors import APIError as APIError, ServiceUnavailableError as ServiceUnavailableError
from typing import Any, Optional

class JetStreamManager:
    def __init__(self, conn, prefix: str = ..., timeout: float = ...) -> None: ...
    async def account_info(self): ...
    async def find_stream_name_by_subject(self, subject: str): ...
    async def stream_info(self, name): ...
    async def add_stream(self, config: api.StreamConfig = ..., **params): ...
    async def delete_stream(self, name): ...
    async def consumer_info(self, stream, consumer, timeout: Any | None = ...): ...
    async def add_consumer(self, stream: str, config: Optional[api.ConsumerConfig] = ..., durable_name: Optional[str] = ..., description: Optional[str] = ..., deliver_subject: Optional[str] = ..., deliver_group: Optional[str] = ..., deliver_policy: Optional[api.DeliverPolicy] = ..., opt_start_seq: Optional[int] = ..., opt_start_time: Optional[int] = ..., ack_policy: Optional[api.AckPolicy] = ..., ack_wait: Optional[int] = ..., max_deliver: Optional[int] = ..., filter_subject: Optional[str] = ..., replay_policy: Optional[api.ReplayPolicy] = ..., sample_freq: Optional[str] = ..., rate_limit_bps: Optional[int] = ..., max_waiting: Optional[int] = ..., max_ack_pending: Optional[int] = ..., flow_control: Optional[bool] = ..., idle_heartbeat: Optional[int] = ..., headers_only: Optional[bool] = ..., timeout: Any | None = ...): ...
    async def delete_consumer(self, stream: Any | None = ..., consumer: Any | None = ...): ...
