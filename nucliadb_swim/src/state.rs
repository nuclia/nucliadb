use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use mio::net::UdpSocket;
use mio::{Events, Interest, Poll, Token};
use serde::*;
use tracing::{debug, error, info, warn};

use super::cluster_config::ClusterConfig;
use super::membership::ArtilleryMemberList;
use crate::errors::*;
use crate::member::{ArtilleryMember, ArtilleryMemberState, ArtilleryStateChange};
use crate::prelude::CONST_PACKET_SIZE;

pub type ArtilleryClusterEvent = (Vec<ArtilleryMember>, ArtilleryMemberEvent);
pub type WaitList = HashMap<SocketAddr, Vec<SocketAddr>>;

#[derive(Debug)]
pub enum ArtilleryMemberEvent {
    Joined(ArtilleryMember),
    WentUp(ArtilleryMember),
    SuspectedDown(ArtilleryMember),
    WentDown(ArtilleryMember),
    Left(ArtilleryMember),
    Payload(ArtilleryMember, String),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ArtilleryMessage {
    node_id: String,
    cluster_key: Vec<u8>,
    node_type: char,
    request: Request,
    state_changes: Vec<ArtilleryStateChange>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
struct EncSocketAddr(SocketAddr);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
enum Request {
    Heartbeat(Option<String>),
    Ack(String),
    Ping(EncSocketAddr, String),
    AckHost(ArtilleryMember),
    Payload(String, String),
}

impl Request {
    fn is_heartbeat(&self) -> bool {
        matches!(self, Request::Heartbeat(_))
    }
}

#[derive(Debug, Clone)]
pub struct TargetedRequest {
    request: Request,
    target: SocketAddr,
    seed: bool,
}

#[derive(Clone)]
pub enum ArtilleryClusterRequest {
    AddSeed(SocketAddr),
    Respond(SocketAddr, ArtilleryMessage),
    React(TargetedRequest),
    LeaveCluster,
    Exit,
    Payload(String, String),
}

const UDP_SERVER: Token = Token(0);

pub struct ArtilleryEpidemic {
    host_id: String,
    config: ClusterConfig,
    node_type: char,
    members: ArtilleryMemberList,
    seed_queue: Vec<SocketAddr>,
    pending_responses: Vec<(Instant, SocketAddr, Vec<ArtilleryStateChange>)>,
    state_changes: Vec<ArtilleryStateChange>,
    wait_list: WaitList,
    server_socket: UdpSocket,
    request_tx: flume::Sender<ArtilleryClusterRequest>,
    event_tx: flume::Sender<ArtilleryClusterEvent>,
    running: AtomicBool,
}

impl Debug for ArtilleryEpidemic {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        fmt.debug_struct("ArtilleryEpidemic")
            .field("host_id", &self.host_id)
            .field("members", &self.members)
            .field("seed_queue", &self.seed_queue)
            .field("pending_responses", &self.pending_responses)
            .field("state_changes", &self.state_changes)
            .finish()
    }
}

pub type ClusterReactor = (Poll, ArtilleryEpidemic);

impl ArtilleryEpidemic {
    pub fn new(
        host_id: String,
        config: ClusterConfig,
        node_type: char,
        event_tx: flume::Sender<ArtilleryClusterEvent>,
        internal_tx: flume::Sender<ArtilleryClusterRequest>,
    ) -> Result<ClusterReactor> {
        let poll: Poll = Poll::new()?;

        let interests = Interest::READABLE.add(Interest::WRITABLE);
        let mut server_socket = UdpSocket::bind(config.listen_addr)?;
        poll.registry()
            .register(&mut server_socket, UDP_SERVER, interests)?;

        let me = ArtilleryMember::current(host_id.clone(), node_type);

        let state = ArtilleryEpidemic {
            host_id,
            config,
            node_type,
            members: ArtilleryMemberList::new(me.clone()),
            seed_queue: Vec::new(),
            pending_responses: Vec::new(),
            state_changes: vec![ArtilleryStateChange::new(me)],
            wait_list: HashMap::new(),
            server_socket,
            request_tx: internal_tx,
            event_tx,
            running: AtomicBool::new(true),
        };

        Ok((poll, state))
    }

    pub(crate) fn event_loop(
        receiver: &mut flume::Receiver<ArtilleryClusterRequest>,
        mut poll: Poll,
        mut state: ArtilleryEpidemic,
    ) -> Result<()> {
        let mut events = Events::with_capacity(1);
        let mut buf = [0_u8; CONST_PACKET_SIZE];

        let mut start = Instant::now();
        let timeout = state.config.ping_interval;

        debug!("Starting Event Loop");
        // Our event loop.
        loop {
            let elapsed = start.elapsed();

            if elapsed >= timeout {
                state.enqueue_seed_nodes();
                state.enqueue_random_ping();
                start = Instant::now();
            }

            if !state.running.load(Ordering::SeqCst) {
                debug!("Stopping artillery epidemic evloop");
                break;
            }

            // Poll to check if we have events waiting for us.
            if let Some(remaining) = timeout.checked_sub(elapsed) {
                poll.poll(&mut events, Some(remaining))?;
            }

            // Process our own events that are submitted to event loop
            // Aka outbound events
            while let Ok(msg) = receiver.try_recv() {
                state.process_internal_request(msg);
            }

            // Process inbound events
            for event in events.iter() {
                if let UDP_SERVER = event.token() {
                    loop {
                        match state.server_socket.recv_from(&mut buf) {
                            Ok((packet_size, source_address)) => {
                                let message: ArtilleryMessage =
                                    serde_json::from_slice(&buf[..packet_size])?;
                                state.request_tx.send(ArtilleryClusterRequest::Respond(
                                    source_address,
                                    message,
                                ))?;
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                // If we get a `WouldBlock` error we know our socket
                                // has no more packets queued, so we can return to
                                // polling and wait for some more.
                                break;
                            }
                            Err(e) => {
                                // If it was any other kind of error, something went
                                // wrong and we terminate with an error.
                                return Err(ArtilleryError::Unexpected(format!(
                                    "Unexpected error occured in event loop: {}",
                                    e
                                )));
                            }
                        }
                    }
                } else {
                    warn!("Got event for unexpected token: {:?}", event);
                }
            }
        }

        info!("Exiting...");
        Ok(())
    }

    fn process_request(&mut self, request: &TargetedRequest) {
        let timeout = Instant::now() + self.config.ping_timeout;
        // It was Ping before
        let should_add_pending = request.request.is_heartbeat();

        let mut state_changed = self.state_changes.clone();
        if request.seed {
            let node_ids: HashSet<_> = state_changed.iter().map(|x| x.member().node_id()).collect();
            for member in self.members.available_nodes() {
                if !node_ids.contains(&member.node_id()) {
                    state_changed.push(ArtilleryStateChange::new(member));
                }
            }
        }

        let message = build_message(
            &self.host_id,
            &self.config.cluster_key,
            self.node_type,
            &request.request,
            &state_changed,
            self.config.network_mtu,
        );

        if should_add_pending {
            self.pending_responses
                .push((timeout, request.target, message.state_changes.clone()));
        }

        let encoded = serde_json::to_string(&message).unwrap();
        debug!("{} SENDS TO {} : {}", self.host_id, request.target, encoded);

        assert!(encoded.len() < self.config.network_mtu);

        let buf = encoded.as_bytes();
        self.server_socket.send_to(buf, request.target).unwrap();
    }

    fn enqueue_seed_nodes(&self) {
        for seed_node in &self.seed_queue {
            match self.members.find_member_by_addr(seed_node) {
                Some(member) => {
                    if member.state() == ArtilleryMemberState::Down {
                        self.request_tx
                            .send(ArtilleryClusterRequest::React(TargetedRequest {
                                request: Request::Heartbeat(None),
                                target: *seed_node,
                                seed: false,
                            }))
                            .unwrap();
                    }
                }
                None => {
                    self.request_tx
                        .send(ArtilleryClusterRequest::React(TargetedRequest {
                            request: Request::Heartbeat(None),
                            target: *seed_node,
                            seed: false,
                        }))
                        .unwrap();
                }
            }
        }
    }

    fn enqueue_random_ping(&mut self) {
        if let Some(member) = self.members.next_random_member() {
            debug!("Random ping to {:?}", member);
            self.request_tx
                .send(ArtilleryClusterRequest::React(TargetedRequest {
                    request: Request::Heartbeat(Some(member.node_id())),
                    target: member.remote_host().unwrap(),
                    seed: false,
                }))
                .unwrap();
        }
    }

    fn prune_timed_out_responses(&mut self) {
        let now = Instant::now();

        let (expired, remaining): (Vec<_>, Vec<_>) = self
            .pending_responses
            .iter()
            .cloned()
            .partition(|&(t, _, _)| t < now);

        let expired_hosts: HashSet<SocketAddr> = expired.iter().map(|&(_, a, _)| a).collect();

        self.pending_responses = remaining;

        let (suspect, down) = self.members.time_out_nodes(&expired_hosts);

        enqueue_state_change(&mut self.state_changes, &down);
        enqueue_state_change(&mut self.state_changes, &suspect);

        for member in suspect {
            self.send_ping_requests(&member);
            self.send_member_event(ArtilleryMemberEvent::SuspectedDown(member.clone()));
        }

        for member in down {
            self.send_member_event(ArtilleryMemberEvent::WentDown(member.clone()));
        }
    }

    fn send_ping_requests(&self, target: &ArtilleryMember) {
        if let Some(target_host) = target.remote_host() {
            for relay in self
                .members
                .hosts_for_indirect_ping(self.config.ping_request_host_count, &target_host)
            {
                debug!("Ping request to {:?} {:?}", target_host, target.node_id());
                self.request_tx
                    .send(ArtilleryClusterRequest::React(TargetedRequest {
                        request: Request::Ping(
                            EncSocketAddr::from_addr(&target_host),
                            target.node_id(),
                        ),
                        target: relay,
                        seed: false,
                    }))
                    .unwrap();
            }
        }
    }

    fn process_internal_request(&mut self, message: ArtilleryClusterRequest) {
        use ArtilleryClusterRequest::*;

        match message {
            AddSeed(addr) => self.seed_queue.push(addr),
            Respond(src_addr, message) => {
                debug!("{} RECV FROM {} {:?}", self.host_id, src_addr, message);
                self.respond_to_message(src_addr, message)
            }
            React(request) => {
                self.prune_timed_out_responses();
                self.process_request(&request);
            }
            LeaveCluster => {
                let myself = self.members.leave();
                enqueue_state_change(&mut self.state_changes, &[myself]);
            }
            Payload(id, msg) => {
                if let Some(target_peer) = self.members.get_member(&id) {
                    if !target_peer.is_remote() {
                        error!("Current node can't send payload to self over LAN");
                        return;
                    }

                    self.process_request(&TargetedRequest {
                        request: Request::Payload(id, msg),
                        target: target_peer
                            .remote_host()
                            .expect("Expected target peer addr"),
                        seed: false,
                    });
                    return;
                }
                warn!(
                    "Unable to find the peer with an id - {} to send the payload",
                    id
                );
            }
            Exit => {
                self.running.store(false, Ordering::SeqCst);
            }
        };
    }

    fn respond_to_message(&mut self, src_addr: SocketAddr, message: ArtilleryMessage) {
        use Request::*;

        if message.cluster_key != self.config.cluster_key {
            error!("Mismatching cluster keys, ignoring message");
            return;
        }
        // We want to abort if a new member has the same node_id of an existing member.
        // A new member is detected according to its socket address.
        let message_cloned = message.clone();
        if !self.members.has_member(&src_addr)
            && self.members.get_member(&message.node_id).is_some()
        {
            error!(
                "Cannot add a member with a node-id `{}` already present in the cluster.",
                message.node_id
            );
            return;
        }

        self.apply_state_changes(message.state_changes, src_addr);

        self.ensure_node_is_member(src_addr, message.node_id, message.node_type);

        let response = match message.request {
            Heartbeat(ref opt_node_id) => {
                let should_ignore_wrong_id = opt_node_id
                    .as_ref()
                    .map(|node_id| node_id != &self.host_id)
                    .unwrap_or(false);
                if should_ignore_wrong_id {
                    None
                } else {
                    Some(TargetedRequest {
                        request: Ack(self.host_id.to_string()),
                        target: src_addr,
                        seed: opt_node_id.is_none(),
                    })
                }
            }
            Ack(node_id) => {
                self.ack_response(src_addr);
                self.mark_node_alive(src_addr, node_id);
                None
            }
            Ping(dest_addr, node_id) => {
                let EncSocketAddr(dest_addr) = dest_addr;
                add_to_wait_list(&mut self.wait_list, &dest_addr, &src_addr);
                Some(TargetedRequest {
                    request: Heartbeat(Some(node_id)),
                    target: dest_addr,
                    seed: false,
                })
            }
            AckHost(member) => {
                self.ack_response(member.remote_host().unwrap());
                self.mark_node_alive(member.remote_host().unwrap(), member.node_id());
                None
            }
            Payload(peer_id, msg) => {
                if let Some(member) = self.members.get_member(&peer_id) {
                    self.send_member_event(ArtilleryMemberEvent::Payload(member, msg));
                } else {
                    warn!("Got payload request from an unknown peer {}", peer_id);
                }
                None
            }
        };
        debug!("Responding {:?} with {:?}", message_cloned, response);

        if let Some(response) = response {
            self.request_tx
                .send(ArtilleryClusterRequest::React(response))
                .unwrap()
        }
    }

    fn ack_response(&mut self, src_addr: SocketAddr) {
        let mut to_remove = Vec::new();

        for &(ref t, ref addr, ref state_changes) in &self.pending_responses {
            if src_addr != *addr {
                continue;
            }

            to_remove.push((*t, *addr, state_changes.clone()));

            self.state_changes.retain(|os| {
                !state_changes
                    .iter()
                    .any(|is| is.member().node_id() == os.member().node_id())
            })
        }

        self.pending_responses
            .retain(|op| !to_remove.iter().any(|ip| ip == op));
    }

    fn ensure_node_is_member(&mut self, src_addr: SocketAddr, node_id: String, node_type: char) {
        if node_id == self.host_id {
            return;
        }
        if self.members.has_member(&src_addr) {
            return;
        }

        let new_member =
            ArtilleryMember::new(node_id, src_addr, 0, node_type, ArtilleryMemberState::Alive);

        self.members.add_member(new_member.clone());

        enqueue_state_change(&mut self.state_changes, &[new_member.clone()]);
        self.send_member_event(ArtilleryMemberEvent::Joined(new_member));
    }

    fn send_member_event(&self, event: ArtilleryMemberEvent) {
        use ArtilleryMemberEvent::*;

        match event {
            Joined(_) | Payload(..) => {}
            WentUp(ref m) => assert_eq!(m.state(), ArtilleryMemberState::Alive),
            WentDown(ref m) => assert_eq!(m.state(), ArtilleryMemberState::Down),
            SuspectedDown(ref m) => assert_eq!(m.state(), ArtilleryMemberState::Suspect),
            Left(ref m) => assert_eq!(m.state(), ArtilleryMemberState::Left),
        };

        // If an error is returned, no one is listening to events anymore. This is normal.
        let _ = self.event_tx.send((self.members.available_nodes(), event));
    }

    fn apply_state_changes(&mut self, state_changes: Vec<ArtilleryStateChange>, from: SocketAddr) {
        let (new, changed) = self.members.apply_state_changes(state_changes, &from);

        enqueue_state_change(&mut self.state_changes, &new);
        enqueue_state_change(&mut self.state_changes, &changed);

        debug!("NEW MEMBERS {:?}", new);

        for member in new {
            self.send_member_event(ArtilleryMemberEvent::Joined(member));
        }

        debug!("CHANGED MEMBERS {:?}", changed);

        for member in changed {
            self.send_member_event(determine_member_event(member));
        }
    }

    fn mark_node_alive(&mut self, src_addr: SocketAddr, node_id: String) {
        if let Some(member) = self.members.mark_node_alive(&src_addr, node_id) {
            if let Some(wait_list) = self.wait_list.get_mut(&src_addr) {
                for remote in wait_list.iter() {
                    self.request_tx
                        .send(ArtilleryClusterRequest::React(TargetedRequest {
                            request: Request::AckHost(member.clone()),
                            target: *remote,
                            seed: false,
                        }))
                        .unwrap();
                }

                wait_list.clear();
            }

            enqueue_state_change(&mut self.state_changes, &[member.clone()]);
            self.send_member_event(ArtilleryMemberEvent::WentUp(member));
        }
    }
}

fn build_message(
    node_id: &str,
    cluster_key: &[u8],
    node_type: char,
    request: &Request,
    state_changes: &[ArtilleryStateChange],
    network_mtu: usize,
) -> ArtilleryMessage {
    let mut message = ArtilleryMessage {
        node_id: node_id.to_string(),
        cluster_key: cluster_key.into(),
        node_type,
        request: request.clone(),
        state_changes: Vec::new(),
    };

    for i in 0..=state_changes.len() {
        message = ArtilleryMessage {
            node_id: node_id.to_string(),
            cluster_key: cluster_key.into(),
            node_type,
            request: request.clone(),
            state_changes: (&state_changes[..i]).to_vec(),
        };

        let encoded = serde_json::to_string(&message).unwrap();
        if encoded.len() >= network_mtu {
            return message;
        }
    }

    message
}

fn add_to_wait_list(wait_list: &mut WaitList, wait_addr: &SocketAddr, notify_addr: &SocketAddr) {
    match wait_list.entry(*wait_addr) {
        Entry::Occupied(mut entry) => {
            entry.get_mut().push(*notify_addr);
        }
        Entry::Vacant(entry) => {
            entry.insert(vec![*notify_addr]);
        }
    };
}

fn determine_member_event(member: ArtilleryMember) -> ArtilleryMemberEvent {
    match member.state() {
        ArtilleryMemberState::Alive => ArtilleryMemberEvent::WentUp(member),
        ArtilleryMemberState::Suspect => ArtilleryMemberEvent::SuspectedDown(member),
        ArtilleryMemberState::Down => ArtilleryMemberEvent::WentDown(member),
        ArtilleryMemberState::Left => ArtilleryMemberEvent::Left(member),
    }
}

fn enqueue_state_change(
    state_changes: &mut Vec<ArtilleryStateChange>,
    members: &[ArtilleryMember],
) {
    for member in members {
        for state_change in state_changes.iter_mut() {
            if state_change.member().node_id() == member.node_id() {
                state_change.update(member.clone());
                return;
            }
        }

        state_changes.push(ArtilleryStateChange::new(member.clone()));
    }
}

impl EncSocketAddr {
    fn from_addr(addr: &SocketAddr) -> Self {
        EncSocketAddr(*addr)
    }
}
