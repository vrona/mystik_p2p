use anyhow::{anyhow, Error as AnyhowError};
use content_hashing::{blakecheck::receive_file_with_hash, get_storage_dir}; // STORAGE_DIR
// use std::io::Write;
use libp2p::kad::KBucketKey;

use std::{
    collections::{hash_map, HashMap},
    sync::Arc,
};

use futures::{
    channel::{mpsc, oneshot},
    //io::{AsyncWriteExt, AsyncReadExt},
    prelude::*,
    Stream as FuturesStream,
    StreamExt,
};

use libp2p::{
    identify,
    identity,
    kad,
    futures::{AsyncWriteExt, AsyncReadExt},
    Multiaddr,
    multiaddr::Protocol,
    PeerId,
    ping,
    relay,
    rendezvous,
    StreamProtocol,
    Stream,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    
};

use libp2p_stream as stream;
//use rand::RngCore;
use tokio::{task::JoinHandle, sync::Mutex, time::{self, Duration}};

use tokio_util::compat::FuturesAsyncReadCompatExt; // try to make compatibility between tokio and libp2p stream

/// A custom protocol tag for our file-exchange streams.
const FILE_PROTOCOL: StreamProtocol = StreamProtocol::new("/file-exchange/1");

/// We define a "flattened" event type so that we can handle both in one `SwarmEvent`.
/// [`NetworkBehaviour`] Composite network behaviour that combines relay, ping, identify, Kademlia (DHT), Streams.
/// and a stream-based file exchange protocol.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CompositeEvent")]
pub struct CompositeBehaviour {
    pub relay: relay::Behaviour,
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub rendezvous: rendezvous::server::Behaviour,
    pub stream: stream::Behaviour,
}

/// Unified event type combining all protocols.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum CompositeEvent {
    Relay(relay::Event),
    Ping(ping::Event),
    Identify(identify::Event),
    Kademlia(kad::Event),
    Rendezvous(rendezvous::server::Event),
    // Note: The stream behaviour does not emit events by default.
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

impl From<rendezvous::server::Event> for CompositeEvent {
    fn from(event: rendezvous::server::Event) -> Self {
        CompositeEvent::Rendezvous(event)
    }
}

impl From<()> for CompositeEvent {
    fn from(_: ()) -> Self {
        CompositeEvent::Stream(())
    }
}

/// Commands sent from `Client` to the `EventLoop`.
#[derive(Debug)]
enum Command {
    StartListening {
        addr: Multiaddr,
        sender: oneshot::Sender<Result<(), AnyhowError>>,
    },
    Advertise{
        peer_id: PeerId,
        address: Multiaddr,
        sender: oneshot::Sender<Result<(), AnyhowError>>,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: oneshot::Sender<Result<(), AnyhowError>>,
    },
    StartProviding {
        file_name: String,
        sender: oneshot::Sender<Result<(), AnyhowError>>,
    },
    GetProviders {
        file_name: String,
        sender: oneshot::Sender<Result<HashMap<PeerId, Vec<Multiaddr>>, AnyhowError>>,
    },
    // -----------------------
    // Stream-based file request:
    RequestFile {
        peer: PeerId,
        file_name: String,
        sender: oneshot::Sender<Result<(Vec<u8>, String), AnyhowError>>,
    },
    // If you want a "RespondFile" command that *pushes* data, you can do something similar.
    // In that scenario, you'd open a substream as well from your side. Alternatively, you
    // could wait for the remote to open a stream, read its file name, and respond with data.
}

/// Events emitted by the `EventLoop` to user-land.
#[derive(Debug)]
pub enum Event {
    /// A peer opened a stream requesting a file. The raw `data` might be the filename
    /// or something else depending on how you implement your protocol.
    InboundFileRequest {
        peer: PeerId,
        file_name: String,
        stream: Stream,
    },
    // Add more events as needed, e.g. inbound/outbound errors, progress, etc.
}

/// Creates the network components, namely:
///
/// 1. The [`Client`] to interact with the network from anywhere in your app.
/// 2. The [`Event`] stream, e.g. for inbound connections.
/// 3. The [`EventLoop`] task driving the network itself.

pub async fn new(secret_key_seed: Option<u8>) -> Result<(Client, impl FuturesStream<Item = Event>, EventLoop), AnyhowError> {
    // Create key pair from seed (or generate one).
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        },
        None => identity::Keypair::generate_ed25519(),
    };
    let peer_id = id_keys.public().to_peer_id();

    // Build a Swarm that uses QUIC and our composite behaviour.
    let swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let relay_behaviour = relay::Behaviour::new(key.public().to_peer_id(), Default::default());
            let ping_behaviour = ping::Behaviour::new(ping::Config::new().with_interval(Duration::from_secs(30)));
            let identify_behaviour = identify::Behaviour::new(identify::Config::new("/composite/0.1.0".to_string(), key.public()));
            let store = kad::store::MemoryStore::new(key.public().to_peer_id());
            let mut kademlia_behaviour = kad::Behaviour::new(peer_id, store);
            kademlia_behaviour.set_mode(Some(kad::Mode::Server));
            let rendezvous_behaviour = rendezvous::server::Behaviour::new(rendezvous::server::Config::default());
            let stream_behaviour = stream::Behaviour::new();
            CompositeBehaviour {
                relay: relay_behaviour,
                ping: ping_behaviour,
                identify: identify_behaviour,
                kademlia: kademlia_behaviour,
                stream: stream_behaviour,
                rendezvous: rendezvous_behaviour,
            }
        })?
        .with_swarm_config(|config| config.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // Set up channels for server commands and event notifications.
    let (command_sender, command_receiver) = mpsc::channel(32);
    let (event_sender, event_receiver) = mpsc::channel(32);

    // Create the shared variable for the actual listen address.
    let actual_listen_addr: Arc<Mutex<Option<Multiaddr>>> = Arc::new(Mutex::new(None));

    Ok((
        Client { sender: command_sender },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender, actual_listen_addr.clone()),
    ))
}

/// The server to control the network layer from your application.
#[derive(Clone)]
pub struct Client {
    sender: mpsc::Sender<Command>,
}

impl Client {
    /// Listen for incoming connections on the given address.
    pub async fn start_listening(
        &mut self,
        addr: Multiaddr,
    ) -> Result<(), AnyhowError> {

        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartListening { addr, sender })
            .await
            .map_err(|e| anyhow!("Failed to send StartListening command: {}", e))?;

        receiver
            .await
            .map_err(|e| anyhow!("StartListening command failed to receive response: {}", e))?
    }

    /// Advertising the local address to the DHT
    pub async fn advertise(
        &mut self,
        peer_id: PeerId,
        address: Multiaddr,
        ) -> Result<(), AnyhowError> {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::Advertise { peer_id, address, sender })
                .await
                .map_err(|e| anyhow!("Failed to send Advertising command: {}", e))?;
            receiver.await.map_err(|e| anyhow!("Advertising command failed: {}", e))?
    }

    /// Dial the given peer at the given address.
    pub async fn dial(
        &mut self,
        peer_id: PeerId,
        peer_addr: Multiaddr,
    ) -> Result<(), AnyhowError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::Dial {
                peer_id,
                peer_addr,
                sender,
            })
            .await
            .map_err(|e| anyhow!("Failed to send Dial command: {}", e))?;
        receiver
            .await
            .map_err(|e| anyhow!("Dial command failed to receive response: {}", e))?
    }

    /// Advertise the local node as the provider of the given file on the DHT.
    pub async fn start_providing(&mut self, file_name: String
        ) -> Result<(), AnyhowError> {
        
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::StartProviding { file_name, sender })
            .await
            .map_err(|e| anyhow!("Failed to send StartProviding command: {}", e))?;
        receiver
            .await
            .map_err(|e| anyhow!("StartProviding command failed to receive response: {}", e))?
    }

    /// Find providers for the given file on the DHT.
    pub async fn get_providers(&mut self, file_name: String) -> Result<HashMap<PeerId, Vec<Multiaddr>>, AnyhowError> {
        // Create a oneshot channel to receive the provider map.
        let (sender, receiver) = oneshot::channel();

        // Send the GetProviders command along with the sender half.
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await
            .map_err(|e| anyhow!("Failed to send GetProviders command: {}", e))?;
        
        // Await the result from the event handler.
        receiver
            .await
            .map_err(|e| anyhow!("GetProviders command failed to receive response: {}", e))?
    }

    // ------------------------------------------------------------------------
    // Below are simple examples of "stream-based" requests for a file.
    // Own logic:
    //   1) Open a stream.
    //   2) Send a "request" (e.g. the file_name).
    //   3) Receive the file's contents (and/or partial chunks).
    //   4) Possibly chunk large data, handle backpressure, etc.
    // ------------------------------------------------------------------------

    /// Open a stream to request file data from `peer`.
    /// Returns the raw bytes of the file.
    pub async fn request_file(
        &mut self,
        peer: PeerId,
        file_name: String,
    ) -> Result<(Vec<u8>, String), AnyhowError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::RequestFile { peer, file_name, sender })
            .await
            .unwrap();
        receiver.await.unwrap()
    }
}

/// Main event loop that drives the Swarm and handles commands from the `Client`.
pub struct EventLoop {
    swarm: Swarm<CompositeBehaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pub actual_listen_addr: Arc<Mutex<Option<Multiaddr>>>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), AnyhowError>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<Result<(), AnyhowError>>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<Result<HashMap<PeerId, Vec<Multiaddr>>, AnyhowError>>>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<CompositeBehaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
        actual_listen_addr: Arc<Mutex<Option<Multiaddr>>>,
    ) -> Self {

        Self {
            swarm,
            command_receiver,
            event_sender,
            actual_listen_addr,
            pending_dial: Default::default(),
            pending_start_providing: Default::default(),
            pending_get_providers: Default::default(),
        }
    }

    /// Spawns the event loop on a tokio task.  
    /// Or you can just call `run()` directly.
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub fn swarm_mut(&mut self) -> &mut libp2p::Swarm<CompositeBehaviour> {
        &mut self.swarm
    }

    pub async fn run(&mut self) {
        // We also spawn an inbound streams handler for the file-exchange protocol.
        // This is optional. You could do it in `handle_event` as well, but spawning
        // a dedicated loop is often clearer.
        let mut incoming = match self.swarm.behaviour().stream.new_control().accept(FILE_PROTOCOL) {
            Ok(incoming) => {
            tracing::debug!("Successfully accepted FILE_PROTOCOL streams.");
            incoming
        }
        Err(e) => {
            tracing::error!("Failed to accept inbound streams for FILE_PROTOCOL: {}", e);
                return;
            }
        };

        // Task that handles inbound streams in parallel, so we don't block the main loop.
        let event_sender_clone = self.event_sender.clone();
        tokio::spawn(async move {
            tracing::debug!("Listening for incoming FILE_PROTOCOL streams.");
            while let Some((peer, stream)) = incoming.next().await {
                tracing::debug!("Received new stream from peer: {}", peer);
                // For demonstration: handle the inbound file request in a new task
                tokio::spawn(Self::handle_inbound_stream(peer, stream, event_sender_clone.clone()));
            }
        });

        loop {
            tokio::select! {
                // Drive the swarm forward.
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event).await;
                },

                // Handle user commands.
                cmd = self.command_receiver.next() => match cmd {
                    Some(c) => self.handle_command(c).await,
                    None => return, // channel closed => shutdown
                },
            }
        }
    }

    pub fn local_peer_id(&self) -> PeerId {
        self.swarm.local_peer_id().clone()
    }

    /// Returns the known multiaddresses for the given `peer_id` by iterating over the routing table.
    pub fn get_peer_addresses(&mut self, target: &PeerId) -> Vec<Multiaddr> {
        
        // Convert your target PeerId into a KBucket key.
        let target_key = KBucketKey::new(target.to_bytes());
        
        let mut addrs = Vec::new();

        // Iterate over all buckets in the routing table.
        for bucket in self.swarm.behaviour_mut().kademlia.kbuckets() {
            // Each bucket contains entries that have a key and associated addresses.
            for entry in bucket.iter() {
                // Compare the entry's peer id with the target.
                if *entry.node.key == target_key {
                    // `entry.node.value()` is typically a collection of Multiaddrs.
                    // The API might vary; adjust as needed.
                    addrs.extend(entry.node.value.iter().cloned());
                }
            }
        }
        addrs
    }

    /// reading from an inbound stream and writing a response.
    async fn handle_inbound_stream(
        peer: PeerId,
        mut stream: Stream,
        mut event_sender: mpsc::Sender<Event>,
    ) {
        // real-world scenario, suppose to:
        //   1) read the "file name" the remote wants
        //   2) find it on disk
        //   3) chunk & send it
        // Here we do a trivial "echo" of whatever we read.
        
        // read the file name from the substream
        tracing::debug!("Inbound: Attempting to read from the stream...");

        let mut buf = Vec::new();

        if let Err(e) = stream.read_to_end(&mut buf).await {
            tracing::debug!("Inbound: Failed to read inbound stream: {e}");
            return;
        } else {
            tracing::debug!("Inbound: Successfully read from the stream. {}.", buf.len());
        }
        let file_name = String::from_utf8_lossy(&buf).to_string();
        tracing::debug!("Inbound: Writing file data for '{file_name}'...");

        // we pass the stream & file-name to the user-land as an event (lib.rs).
        tracing::debug!("Emitting InboundFileRequest event for file '{file_name}' from peer {peer}");
        if let Err(e) = event_sender.send(Event::InboundFileRequest { peer, file_name, stream }).await {
            tracing::debug!("Failed to send inbound stream event: {e}");
        }
    }

    async fn handle_swarm_event(&mut self, event: SwarmEvent<CompositeEvent>) {


        match event {

            SwarmEvent::Behaviour(CompositeEvent::Rendezvous(
                rendezvous::server::Event::PeerRegistered { peer, registration },
            )) => {
                tracing::info!(
                    "Peer {} registered for namespace '{}'",
                    peer,
                    registration.namespace,
                );
            }
            SwarmEvent::Behaviour(CompositeEvent::Rendezvous(
                rendezvous::server::Event::DiscoverServed { enquirer, registrations },
            )) => {
                tracing::info!(
                    "Served peer {} with {} registrations",
                    enquirer,
                    registrations.len(),
                );
                let local_peer_id = self.swarm.local_peer_id().clone();

                        for registration in registrations {
                            for address in registration.record.addresses() {

                                let peer = registration.record.peer_id();
                                //tracing::info!(%peer, %address, "Discovered peer");

                                if peer == local_peer_id {
                                    //tracing::info!(%peer, "Skipping dialing self");
                                    continue;
                                }

                                let p2p_suffix = Protocol::P2p(peer);
                                let address_with_p2p =
                                    if !address.ends_with(&Multiaddr::empty().with(p2p_suffix.clone())) {
                                        address.clone().with(p2p_suffix.clone())
                                    } else {
                                        address.clone()
                                    };
                                
                                //println!("YOUR NEIGHBORS \n {} \n", address_with_p2p);
                                //if let Err(e) =
                                let _ = self.swarm.dial(address_with_p2p.clone()); //{
                                //    tracing::error!("Dial failed: {}", e);
                                //}
                                self.swarm.behaviour_mut().kademlia.add_address(&peer, address_with_p2p);
                            }
                        }
            }
            
            SwarmEvent::Behaviour(CompositeEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result: kad::QueryResult::StartProviding(_),
                    ..
                },
            )) => {
                // Mark the providing request as completed.
                if let Some(sender) = self.pending_start_providing.remove(&id) {
                let _ = sender.send(Ok(()));
                    print!("Providing request completed.");
                }
            }
            SwarmEvent::Behaviour(CompositeEvent::Kademlia(
                kad::Event::OutboundQueryProgressed {
                    id,
                    result:
                        kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                            providers,
                            ..
                        })),
                    ..
                },
            )) => {
                if let Some(sender) = self.pending_get_providers.remove(&id) {
                    let mut provider_addrs = HashMap::new();
                    // Iterate over the discovered providers and print their known addresses.
                    for provider in &providers {
                        let addrs = self.get_peer_addresses(provider);
                        provider_addrs.insert(*provider, addrs.clone());
                        //tracing::info!("Provider {} has addresses: {:?}", provider, addrs);
                    }
                    let _ = sender.send(Ok(provider_addrs));
                    // Stop the query after first found set of providers.
                    self.swarm.behaviour_mut().kademlia.query_mut(&id).map(|mut q| q.finish());
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = self.swarm.local_peer_id().clone();
                let full_addr = address.with(Protocol::P2p(local_peer_id));
                
                // Update the shared state with the actual bound address.
                *self.actual_listen_addr.lock().await = Some(full_addr.clone());
                println!("Local node is listening on {:?}", *self.actual_listen_addr.lock().await);
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                if endpoint.is_dialer() {
                    // Handle any pending dial command first.
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Ok(()));
                    }
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                if let Some(peer_id) = peer_id {
                    if let Some(sender) = self.pending_dial.remove(&peer_id) {
                        let _ = sender.send(Err(anyhow!("Outgoing connection error to {}: {}", peer_id, error)));
                    }
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
            tracing::info!("Disconnected from {}", peer_id);
            }
            other => {
                tracing::debug!("Unhandled event: {:?}", other);
            }
        }
    }

    async fn handle_command(&mut self, command: Command) {
        match command {
            Command::StartListening { addr, sender } => {

                let result = self.swarm
                    .listen_on(addr)
                    .map(|_listener_id| ())
                    .map_err(|e| anyhow!("Failed to listen on address: {}", e));
                let _ = sender.send(result);
            }
            Command::Advertise {
                peer_id,
                address,
                sender
            } => {
                
                self.swarm.add_external_address(address.clone());
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, address);
                let _ = sender.send(Ok(()));
            }
            Command::Dial {
                peer_id,
                peer_addr,
                sender,
            } => {
                if let hash_map::Entry::Vacant(e) = self.pending_dial.entry(peer_id) {
                    
                    // Add to kademlia so we remember addresses for peer.
                    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, peer_addr.clone());

                    // Actually dial it:
                    let dial_res = self.swarm.dial(peer_addr.with(Protocol::P2p(peer_id)));
                    match dial_res {
                        Ok(()) => {
                            e.insert(sender);
                        }
                        Err(e) => {
                            let _ = sender.send(Err(anyhow!("Failed to dial peer {}: {}", peer_id, e)));
                        }
                    }
                } else {
                    // Already dialing this peer. Possibly return an error or ignore.
                    let _ = sender.send(Err(anyhow!("Already dialing peer {}", peer_id)));
                }
            }
            Command::StartProviding { file_name, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .start_providing(file_name.clone().into_bytes().into())
                    .map_err(|e| anyhow!("Failed to start providing file '{}': {}", file_name, e))
                    .expect("No store error.");
                self.pending_start_providing.insert(query_id, sender);
            }
            Command::GetProviders { file_name, sender } => {
                let query_id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .get_providers(file_name.into_bytes().into());
                self.pending_get_providers.insert(query_id, sender);
            }
            Command::RequestFile { peer, file_name, sender } => {

                let mut control = self.swarm.behaviour_mut().stream.new_control();
                let open_result = control.open_stream(peer, FILE_PROTOCOL).await;
                let res =
                    match open_result {
                        Ok(stream) => {
                            // tracing::debug!("Successfully opened stream to peer: {}", peer);
                            tracing::debug!("Writing file name '{}' to the stream...", file_name);

                            // Split the stream into read and write halves
                            let (reader, mut writer) = stream.split();

                            // Conversion from libp2p to tokio
                            let reader = reader.compat(); 
                            if let Err(e) = writer.write_all(file_name.as_bytes()).await {
                                //tracing::debug!("Failed to write file name to stream: {e}");
                                Err(anyhow!("Failed to write file name to stream: {}", e))

                            } else {

                                tracing::debug!("Written file name '{}' to stream.", file_name);
                                // 2) Shutdown the write side to signal EOF
                                if let Err(e) = writer.flush().await {
                                    //tracing::debug!("Failed to shutdown write half of the stream: {e}");
                                    Err(anyhow!("Failed to shutdown write half of the stream: {}", e))
                                    
                                } else {
                                    writer.close().await.unwrap();

                                    tracing::debug!("Write side of the stream successfully closed.");

                                    let directory_path = format!("{}", get_storage_dir());
                                    let dir_path = std::path::Path::new(&directory_path);
                                    //tracing::debug!("Calling receive_file_with_hash for file: {:?}", dir_path);

                                    match receive_file_with_hash(reader, dir_path).await {
                                        Ok(cid) => {
                                            tracing::debug!("File received successfully at {:?}", dir_path);
                                            
                                            let final_path = format!("{}{}.bin", dir_path.display(), cid);

                                            match std::fs::read(final_path) {
                                                Ok(file_bytes) => {
                                                    //tracing::debug!("File read into memory successfully. Bytes: {}", file_bytes.len());
                                                    Ok((file_bytes, cid))
                                                }
                                                Err(e) => {
                                                    //tracing::debug!("Failed to read file from disk: {e}");
                                                    Err(anyhow!("Failed to read file from disk: {}", e))
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            //tracing::debug!("Error during receive_file_with_hash: {e}");
                                            Err(anyhow!("Error during receive_file_with_hash: {}", e))
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            //tracing::debug!("Failed to open stream to peer {}: {e}", peer);
                            Err(anyhow!("Failed to open stream to peer {}: {}", peer, e))
                        }
                    };

                let _ = sender.send(res);
            }
        }
    }
}