from enum import Enum
from typing import Any, Dict, List, Optional

MsgIdHdr: str
ExpectedStreamHdr: str
ExpectedLastSeqHdr: str
ExpectedLastSubjSeqHdr: str
ExpectedLastMsgIdHdr: str
MsgRollup: str
LastConsumerSeqHdr: str
LastStreamSeqHdr: str
StatusHdr: str
DescHdr: str
DefaultPrefix: str
InboxPrefix: Any
ServiceUnavailableStatus: str
NoMsgsStatus: str
RequestTimeoutStatus: str
CtrlMsgStatus: str

class Base:
    @classmethod
    def properties(klass, **opts): ...
    @classmethod
    def loads(klass, **opts): ...
    def asjson(self) -> str: ...

class PubAck(Base):
    stream: str
    seq: int
    domain: Optional[str]
    duplicate: Optional[bool]
    def __init__(self, stream, seq, domain, duplicate) -> None: ...

class Placement(Base):
    cluster: str
    tags: Optional[List[str]]
    def __init__(self, cluster, tags) -> None: ...

class ExternalStream(Base):
    api: str
    deliver: Optional[str]
    def __init__(self, api, deliver) -> None: ...

class StreamSource(Base):
    name: str
    opt_start_seq: Optional[int]
    filter_subject: Optional[str]
    external: Optional[ExternalStream]
    def __post_init__(self) -> None: ...
    def __init__(self, name, opt_start_seq, filter_subject, external) -> None: ...

class StreamSourceInfo(Base):
    name: str
    lag: Optional[int]
    active: Optional[int]
    error: Optional[Dict[str, Any]]
    def __init__(self, name, lag, active, error) -> None: ...

class LostStreamData(Base):
    msgs: Optional[List[int]]
    bytes: Optional[int]
    def __init__(self, msgs, bytes) -> None: ...

class StreamState(Base):
    messages: int
    bytes: int
    first_seq: int
    last_seq: int
    consumer_count: int
    deleted: Optional[List[int]]
    num_deleted: Optional[int]
    lost: Optional[LostStreamData]
    def __post_init__(self) -> None: ...
    def __init__(self, messages, bytes, first_seq, last_seq, consumer_count, deleted, num_deleted, lost) -> None: ...

class RetentionPolicy(str, Enum):
    limits: str
    interest: str
    workqueue: str

class StorageType(str, Enum):
    file: str
    memory: str

class DiscardPolicy(str, Enum):
    old: str
    new: str

class StreamConfig(Base):
    name: Optional[str]
    description: Optional[str]
    subjects: Optional[List[str]]
    retention: Optional[RetentionPolicy]
    max_consumers: Optional[int]
    max_msgs: Optional[int]
    max_bytes: Optional[int]
    discard: Optional[DiscardPolicy]
    max_age: Optional[int]
    max_msgs_per_subject: Optional[int]
    max_msg_size: Optional[int]
    storage: Optional[StorageType]
    num_replicas: Optional[int]
    no_ack: Optional[bool]
    template_owner: Optional[str]
    duplicate_window: Optional[int]
    placement: Optional[Placement]
    mirror: Optional[StreamSource]
    sources: Optional[List[StreamSource]]
    sealed: Optional[bool]
    deny_delete: Optional[bool]
    deny_purge: Optional[bool]
    allow_rollup_hdrs: Optional[bool]
    def __post_init__(self) -> None: ...
    def __init__(self, name, description, subjects, retention, max_consumers, max_msgs, max_bytes, discard, max_age, max_msgs_per_subject, max_msg_size, storage, num_replicas, no_ack, template_owner, duplicate_window, placement, mirror, sources, sealed, deny_delete, deny_purge, allow_rollup_hdrs) -> None: ...

class PeerInfo(Base):
    name: Optional[str]
    current: Optional[bool]
    offline: Optional[bool]
    active: Optional[int]
    lag: Optional[int]
    def __init__(self, name, current, offline, active, lag) -> None: ...

class ClusterInfo(Base):
    leader: Optional[str]
    name: Optional[str]
    replicas: Optional[List[PeerInfo]]
    def __post_init__(self) -> None: ...
    def __init__(self, leader, name, replicas) -> None: ...

class StreamInfo(Base):
    config: StreamConfig
    state: StreamState
    mirror: Optional[StreamSourceInfo]
    sources: Optional[List[StreamSourceInfo]]
    cluster: Optional[ClusterInfo]
    did_create: Optional[bool]
    def __post_init__(self) -> None: ...
    def __init__(self, config, state, mirror, sources, cluster, did_create) -> None: ...

class AckPolicy(str, Enum):
    none: str
    all: str
    explicit: str

class DeliverPolicy(str, Enum):
    all: str
    last: str
    new: str
    last_per_subject: str
    by_start_sequence: str
    by_start_time: str

class ReplayPolicy(str, Enum):
    instant: str
    original: str

class ConsumerConfig(Base):
    durable_name: Optional[str]
    description: Optional[str]
    deliver_subject: Optional[str]
    deliver_group: Optional[str]
    deliver_policy: Optional[DeliverPolicy]
    opt_start_seq: Optional[int]
    opt_start_time: Optional[int]
    ack_policy: Optional[AckPolicy]
    ack_wait: Optional[int]
    max_deliver: Optional[int]
    filter_subject: Optional[str]
    replay_policy: Optional[ReplayPolicy]
    sample_freq: Optional[str]
    rate_limit_bps: Optional[int]
    max_waiting: Optional[int]
    max_ack_pending: Optional[int]
    flow_control: Optional[bool]
    idle_heartbeat: Optional[int]
    headers_only: Optional[bool]
    def __post_init__(self) -> None: ...
    def __init__(self, durable_name, description, deliver_subject, deliver_group, deliver_policy, opt_start_seq, opt_start_time, ack_policy, ack_wait, max_deliver, filter_subject, replay_policy, sample_freq, rate_limit_bps, max_waiting, max_ack_pending, flow_control, idle_heartbeat, headers_only) -> None: ...

class SequenceInfo(Base):
    consumer_seq: int
    stream_seq: int
    def __init__(self, consumer_seq, stream_seq) -> None: ...

class ConsumerInfo(Base):
    stream_name: str
    config: ConsumerConfig
    delivered: Optional[SequenceInfo]
    ack_floor: Optional[SequenceInfo]
    num_ack_pending: Optional[int]
    num_redelivered: Optional[int]
    num_waiting: Optional[int]
    num_pending: Optional[int]
    name: Optional[str]
    cluster: Optional[ClusterInfo]
    push_bound: Optional[bool]
    def __post_init__(self) -> None: ...
    def __init__(self, stream_name, config, delivered, ack_floor, num_ack_pending, num_redelivered, num_waiting, num_pending, name, cluster, push_bound) -> None: ...

class AccountLimits(Base):
    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int
    def __init__(self, max_memory, max_storage, max_streams, max_consumers) -> None: ...

class APIStats(Base):
    total: int
    errors: int
    def __init__(self, total, errors) -> None: ...

class AccountInfo(Base):
    memory: int
    storage: int
    streams: int
    consumers: int
    limits: AccountLimits
    api: APIStats
    domain: Optional[str]
    def __post_init__(self) -> None: ...
    def __init__(self, memory, storage, streams, consumers, limits, api, domain) -> None: ...

class RawStreamMsg(Base):
    subject: Optional[str]
    seq: Optional[int]
    data: Optional[bytes]
    hdrs: Optional[bytes]
    headers: Optional[dict]
    @property
    def sequence(self): ...
    def __init__(self, subject, seq, data, hdrs, headers) -> None: ...

class KeyValueConfig(Base):
    bucket: Optional[str]
    description: Optional[str]
    max_value_size: Optional[int]
    history: Optional[int]
    ttl: Optional[int]
    max_bytes: Optional[int]
    storage: Optional[StorageType]
    replicas: Optional[int]
    def __init__(self, bucket, description, max_value_size, history, ttl, max_bytes, storage, replicas) -> None: ...
