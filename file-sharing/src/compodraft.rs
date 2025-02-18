use futures::prelude::*;
use libp2p::{
    core::multiaddr::Protocol,
    identity,
    kad,
    ping,
    relay,
    rendezvous,
    identify,
    Multiaddr,
    PeerId,
    StreamProtocol,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent, SwarmBuilder},
};
use libp2p_stream as stream; // Your file-exchange protocol.
use std::{error::Error, time::Duration};
use tokio::runtime::Runtime;
use tracing_subscriber;

/// A custom protocol tag for our file-exchange streams.
const FILE_PROTOCOL: StreamProtocol = StreamProtocol::new("/file-exchange/1");

/// CompositeBehaviour combines relay, ping, identify, Kademlia (DHT), file exchange, and rendezvous.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CompositeEvent")]
pub struct CompositeBehaviour {
    pub relay: relay::Behaviour,
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub stream: stream::Behaviour,
    pub rendezvous: rendezvous::client::Behaviour,
}

/// CompositeEvent unifies events from all sub-protocols.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum CompositeEvent {
    Relay(relay::Event),
    Ping(ping::Event),
    Identify(identify::Event),
    Kademlia(kad::Event),
    Rendezvous(rendezvous::client::Event),
    // libp2p_stream::Behaviour does not produce events by default.
    Stream(()),
}

impl From<relay::Event> for CompositeEvent {
    fn from(event: relay::Event) -> Self {
        CompositeEvent::Relay(event)
    }
}

impl From<ping::Event> for CompositeEvent {
    fn from(event: ping::Event) -> Self {
        CompositeEvent::Ping(event)
    }
}

impl From<identify::Event> for CompositeEvent {
    fn from(event: identify::Event) -> Self {
        CompositeEvent::Identify(event)
    }
}

impl From<kad::Event> for CompositeEvent {
    fn from(event: kad::Event) -> Self {
        CompositeEvent::Kademlia(event)
    }
}

impl From<rendezvous::client::Event> for CompositeEvent {
    fn from(event: rendezvous::client::Event) -> Self {
        CompositeEvent::Rendezvous(event)
    }
}

impl From<()> for CompositeEvent {
    fn from(_: ()) -> Self {
        CompositeEvent::Stream(())
    }
}

/// Client API to control the network.
#[derive(Clone)]
pub struct Client {
    pub sender: futures::channel::mpsc::Sender<Command>,
}

impl Client {
    pub async fn start_listening(&mut self, addr: Multiaddr) -> Result<(), Box<dyn Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await?;
        receiver.await?
    }

    pub async fn dial(&mut self, peer_id: PeerId, peer_addr: Multiaddr) -> Result<(), Box<dyn Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        self.sender
            .send(Command::Dial { peer_id, peer_addr, sender })
            .await?;
        receiver.await?
    }

    pub async fn start_providing(&mut self, file_name: String) -> Result<(), Box<dyn Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        self.sender
            .send(Command::StartProviding { file_name, sender })
            .await?;
        receiver.await?
    }

    pub async fn get_providers(&mut self, file_name: String) -> Result<std::collections::HashSet<PeerId>, Box<dyn Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await?;
        receiver.await?
    }

    pub async fn request_file(&mut self, peer: PeerId, file_name: String) -> Result<Vec<u8>, Box<dyn Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        self.sender
            .send(Command::RequestFile { peer, file_name, sender })
            .await?;
        receiver.await?
    }
}

/// Commands sent from the client.
#[derive(Debug)]
enum Command {
    StartListening {
        addr: Multiaddr,
        sender: futures::channel::oneshot::Sender<Result<(), Box<dyn Error>>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: futures::channel::oneshot::Sender<Result<(), Box<dyn Error>>>,
    },
    StartProviding {
        file_name: String,
        sender: futures::channel::oneshot::Sender<Result<(), Box<dyn Error>>>,
    },
    GetProviders {
        file_name: String,
        sender: futures::channel::oneshot::Sender<Result<std::collections::HashSet<PeerId>, Box<dyn Error>>>,
    },
    RequestFile {
        peer: PeerId,
        file_name: String,
        sender: futures::channel::oneshot::Sender<Result<Vec<u8>, Box<dyn Error>>>,
    },
}

/// Events emitted by the network.
#[derive(Debug)]
pub enum Event {
    InboundFileRequest {
        peer: PeerId,
        file_name: String,
        stream: stream::Stream,
    },
    // Additional events can be added.
}

/// The EventLoop that drives the swarm.
pub struct EventLoop {
    pub swarm: Swarm<CompositeBehaviour>,
    pub command_receiver: futures::channel::mpsc::Receiver<Command>,
    pub event_sender: futures::channel::mpsc::Sender<Event>,
    pub pending_dial: std::collections::HashMap<PeerId, futures::channel::oneshot::Sender<Result<(), Box<dyn Error>>>>,
    pub pending_start_providing: std::collections::HashMap<kad::QueryId, futures::channel::oneshot::Sender<Result<(), Box<dyn Error>>>>,
    pub pending_get_providers: std::collections::HashMap<kad::QueryId, futures::channel::oneshot::Sender<Result<std::collections::HashSet<PeerId>, Box<dyn Error>>>>,
}

impl EventLoop {
    pub fn new(
        swarm: Swarm<CompositeBehaviour>,
        command_receiver: futures::channel::mpsc::Receiver<Command>,
        event_sender: futures::channel::mpsc::Sender<Event>,
    ) -> Self {
        Self {
            swarm,
            command_receiver,
            event_sender,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
        }
    }

    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub async fn run(&mut self) {
        // Spawn a task to handle inbound file streams.
        let mut incoming = match self.swarm.behaviour().stream.new_control().accept(FILE_PROTOCOL) {
            Ok(i) => i,
            Err(e) => {
                tracing::error!("Failed to accept inbound streams: {}", e);
                return;
            }
        };
        let event_sender_clone = self.event_sender.clone();
        tokio::spawn(async move {
            while let Some((peer, stream)) = incoming.next().await {
                tokio::spawn(Self::handle_inbound_stream(peer, stream, event_sender_clone.clone()));
            }
        });

        loop {
            tokio::select! {
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                },
                cmd = self.command_receiver.next() => {
                    if let Some(c) = cmd {
                        self.handle_command(c).await;
                    } else {
                        return;
                    }
                }
            }
        }
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.swarm.local_peer_id().clone()
    }

    pub fn get_peer_addresses(&mut self, target: &PeerId) -> Vec<Multiaddr> {
        let target_key = kad::record::Key::new(target.to_bytes());
        let mut addrs = Vec::new();
        for bucket in self.swarm.behaviour_mut().kademlia.kbuckets() {
            for entry in bucket.iter() {
                if *entry.node.key == kad::record::Key::new(target.to_bytes()) {
                    addrs.extend(entry.node.value.iter().cloned());
                }
            }
        }
        addrs
    }

    async fn handle_inbound_stream(peer: PeerId, mut stream: stream::Stream, mut event_sender: futures::channel::mpsc::Sender<Event>) {
        tracing::debug!("Inbound: reading from stream from peer {}", peer);
        let mut buf = Vec::new();
        if stream.read_to_end(&mut buf).await.is_err() {
            tracing::debug!("Failed to read inbound stream from {}", peer);
            return;
        }
        let file_name = String::from_utf8_lossy(&buf).to_string();
        tracing::debug!("Inbound: got file request for '{}'", file_name);
        if event_sender.send(Event::InboundFileRequest { peer, file_name, stream }).await.is_err() {
            tracing::debug!("Failed to send inbound file event");
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<CompositeEvent>) {
        match event {
            SwarmEvent::Behaviour(CompositeEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::StartProviding(_),
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_start_providing.remove(&id) {
                    let _ = sender.send(Ok(()));
                }
            },
            SwarmEvent::Behaviour(CompositeEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders { providers, .. })),
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_get_providers.remove(&id) {
                    let _ = sender.send(Ok(providers));
                    self.swarm.behaviour_mut().kademlia.query_mut(&id).map(|mut q| q.finish());
                }
            },
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("Local node is listening on {:?}", address.with(Protocol::P2p(*self.swarm.local_peer_id())));
            },
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                if endpoint.is_dialer() {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                    }
                }
            },
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(anyhow::anyhow!("Outgoing connection error to {}: {}", peer_id, error)));
                    }
                }
            },
            _ => {}
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {
                let res = self.swarm.listen_on(addr).map(|_| ()).map_err(|e| anyhow::anyhow!("Failed to listen: {}", e));
                let _ = sender.send(res);
            },
            Command::Dial { peer_id, peer_addr, sender } => {
                use std::collections::hash_map::Entry;
                if let Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, peer_addr.clone());
                    let dial_res = self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id)));
                    match dial_res {
                        Ok(()) => { e.insert(sender); },
                        Err(e) => { let _ = sender.send(Err(anyhow::anyhow!("Dial failed for {}: {}", peer_id, e))); },
                    }
                } else {
                    let _ = sender.send(Err(anyhow::anyhow!("Already dialing {}", peer_id)));
                }
            },
            Command::StartProviding { file_name, sender } => {
                let query_id = self.swarm.behaviour_mut().kademlia.start_providing(file_name.clone().into_bytes().into())
                    .map_err(|e| anyhow::anyhow!("Failed to start providing '{}': {}", file_name, e))
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            },
            Command::GetProviders { file_name, sender } => {
                let query_id = self.swarm.behaviour_mut().kademlia.get_providers(file_name.into_bytes().into());
                self.pending_get_providers.insert(query_id, sender);
            },
            Command::RequestFile { peer, file_name, sender } => {
                let mut control = self.swarm.behaviour_mut().stream.new_control();
                let open_result = control.open_stream(peer, FILE_PROTOCOL).await;
                let res = match open_result {
                    Ok(stream) => {
                        tracing::debug!("Opened stream to {} for file '{}'", peer, file_name);
                        let (reader, mut writer) = stream.split();
                        if writer.write_all(file_name.as_bytes()).await.is_err() {
                            Err(anyhow::anyhow!("Failed to write file name"))
                        } else if writer.flush().await.is_err() {
                            Err(anyhow::anyhow!("Failed to flush stream"))
                        } else {
                            writer.close().await.unwrap();
                            let raw_final_path = format!("data/{}", file_name);
                            let final_path = std::path::Path::new(&raw_final_path);
                            match crate::blakecheck::receive_file_with_hash(reader, final_path).await {
                                Ok(()) => std::fs::read(final_path).map_err(|e| anyhow::anyhow!("Failed to read file: {}", e)),
                                Err(e) => Err(anyhow::anyhow!("receive_file_with_hash error: {}", e)),
                            }
                        }
                    },
                    Err(e) => Err(anyhow::anyhow!("Open stream failed: {}", e)),
                };
                let _ = sender.send(res);
            },
        }
    }
}

/// Creates the composite node components.
/// This function builds the swarm with our composite behaviour (which now includes rendezvous),
/// sets the idle connection timeout, and returns the client, event stream, and event loop.
pub async fn new(secret_key_seed: Option<u8>) -> Result<(Client, impl futures::Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    // Create identity.
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        },
        None => identity::Keypair::generate_ed25519(),
    };
    let peer_id = id_keys.public().to_peer_id();

    // Build a Swarm using QUIC and our composite behaviour.
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let relay_behaviour = relay::Behaviour::new(key.public().to_peer_id(), Default::default());
            let ping_behaviour = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(10)));
            let identify_behaviour = identify::Behaviour::new(identify::Config::new(
                "/composite/0.1.0".to_string(),
                key.public(),
            ));
            let store = kad::store::MemoryStore::new(key.public().to_peer_id());
            let mut kademlia_behaviour = kad::Behaviour::new(peer_id, store);
            kademlia_behaviour.set_mode(Some(kad::Mode::Server));
            let stream_behaviour = stream::Behaviour::new();
            let rendezvous_behaviour = rendezvous::client::Behaviour::new(key.clone());
            CompositeBehaviour {
                relay: relay_behaviour,
                ping: ping_behaviour,
                identify: identify_behaviour,
                kademlia: kademlia_behaviour,
                stream: stream_behaviour,
                rendezvous: rendezvous_behaviour,
            }
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    let (command_sender, command_receiver) = futures::channel::mpsc::channel(32);
    let (event_sender, event_receiver) = futures::channel::mpsc::channel(32);

    Ok((
        Client { sender: command_sender },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}
