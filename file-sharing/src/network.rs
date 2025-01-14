use anyhow::{anyhow, Error as AnyhowError};
use crate::chunkblake::receive_file_with_hash;
// use std::io::Write;

use std::{
    collections::{hash_map, HashMap, HashSet}, // , error::Error as StdError
    time::Duration
};

use futures::{
    channel::{mpsc, oneshot},
    //io::{AsyncWriteExt, AsyncReadExt},
    prelude::*,
    Stream as FuturesStream,
    StreamExt,
};

use libp2p::{
    futures::{AsyncWriteExt, AsyncReadExt},
    identity,
    kad,
    Multiaddr,
    multiaddr::Protocol,
    PeerId,
    StreamProtocol,
    Stream,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    
};

use libp2p_stream as stream;
//use rand::RngCore;
use tokio::task::JoinHandle;

/// A custom protocol tag for our file-exchange streams.
const FILE_PROTOCOL: StreamProtocol = StreamProtocol::new("/file-exchange/1");

/// Creates the network components, namely:
///
/// 1. The [`Client`] to interact with the network from anywhere in your app.
/// 2. The [`Event`] stream, e.g. for inbound connections.
/// 3. The [`EventLoop`] task driving the network itself.

pub(crate) async fn new(
    secret_key_seed: Option<u8>,
) -> Result<(Client, impl FuturesStream<Item = Event>, EventLoop), AnyhowError> {
    // Create a public/private key pair, either random or based on a seed.
    let id_keys = match secret_key_seed {
        Some(seed) => {
            let mut bytes = [0u8; 32];
            bytes[0] = seed;
            identity::Keypair::ed25519_from_bytes(bytes).unwrap()
        }
        None => identity::Keypair::generate_ed25519(),
    };
    let peer_id = id_keys.public().to_peer_id();

    // Build a Swarm that uses QUIC (or TCP+Noise+Yamux).
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(id_keys)
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| Behaviour {
            kademlia: kad::Behaviour::new(
                peer_id,
                kad::store::MemoryStore::new(key.public().to_peer_id()),
            ),
            stream: stream::Behaviour::new(),
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // For Kademlia we act in server mode.
    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    let (command_sender, command_receiver) = mpsc::channel(32);
    let (event_sender, event_receiver) = mpsc::channel(32);

    Ok((
        Client {
            sender: command_sender,
        },
        event_receiver,
        EventLoop::new(swarm, command_receiver, event_sender),
    ))
}

/// The client to control the network layer from your application.
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
    pub async fn get_providers(&mut self, file_name: String) -> Result<HashSet<PeerId>, AnyhowError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Command::GetProviders { file_name, sender })
            .await
            .map_err(|e| anyhow!("Failed to send GetProviders command: {}", e))?;
        
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
    ) -> Result<Vec<u8>, AnyhowError> {
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
    swarm: Swarm<Behaviour>,
    command_receiver: mpsc::Receiver<Command>,
    event_sender: mpsc::Sender<Event>,
    pending_dial: HashMap<PeerId, oneshot::Sender<Result<(), AnyhowError>>>,
    pending_start_providing: HashMap<kad::QueryId, oneshot::Sender<Result<(), AnyhowError>>>,
    pending_get_providers: HashMap<kad::QueryId, oneshot::Sender<Result<HashSet<PeerId>, AnyhowError>>>,
}

impl EventLoop {
    fn new(
        swarm: Swarm<Behaviour>,
        command_receiver: mpsc::Receiver<Command>,
        event_sender: mpsc::Sender<Event>,
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

    /// Spawns the event loop on a tokio task.  
    /// Or you can just call `run()` directly.
    pub fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
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
                }
                // Handle user commands.
                cmd = self.command_receiver.next() => match cmd {
                    Some(c) => self.handle_command(c).await,
                    None => return, // channel closed => shutdown
                },
            }
        }
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

    async fn handle_swarm_event(&mut self, event: SwarmEvent<BehaviourEvent>) {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
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
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(
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
                    let _ = sender.send(Ok(providers));
                    // Stop the query after first found set of providers.
                    self.swarm.behaviour_mut().kademlia.query_mut(&id).map(|mut q| q.finish());
                }
            }
            SwarmEvent::NewListenAddr { address, .. } => {
                let local_peer_id = *self.swarm.local_peer_id();
                println!(
                    "Local node is listening on {:?}",
                    address.with(Protocol::P2p(local_peer_id))
                );
            }
            SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                if endpoint.is_dialer() {
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
            _ => {}
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
                            tracing::debug!("Successfully opened stream to peer: {}", peer);
                            tracing::debug!("Writing file name '{}' to the stream...", file_name);

                            // Split the stream into read and write halves
                            let (reader, mut writer) = stream.split();

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

                                    let raw_final_path = format!("/Volumes/vrona_SSD/intnet/output_test/{}", file_name);
                                    let final_path = std::path::Path::new(&raw_final_path);
                                    tracing::debug!("Calling receive_file_with_hash for file: {:?}", final_path);

                                    match receive_file_with_hash(reader, final_path).await {
                                        Ok(_) => {
                                            tracing::debug!("File received successfully at {:?}", final_path);
                                            match std::fs::read(final_path) {
                                                Ok(file_bytes) => {
                                                    tracing::debug!("File read into memory successfully. Bytes: {}", file_bytes.len());
                                                    Ok(file_bytes)
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
                            tracing::debug!("Failed to open stream to peer {}: {e}", peer);
                            Err(anyhow!("Failed to open stream to peer {}: {}", peer, e))
                        }
                    };
                let _ = sender.send(res);
            }
        }
    }
}

/// Our libp2p [`NetworkBehaviour`] combining Kademlia + Streams.
/// We define a “flattened” event type so that we can handle both in one `SwarmEvent`.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "BehaviourEvent")]
struct Behaviour {
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub stream: stream::Behaviour,
}

/// We unify the two events (`kad` + `stream`) into one for the `SwarmEvent`.
#[allow(clippy::large_enum_variant)]
enum BehaviourEvent {
    Kademlia(kad::Event),
    // Note that `libp2p_stream::Behaviour` does **not** emit events by default
    // unless you enable them. For now, we do not handle them here. We accept
    // substreams ourselves.
    StreamNoop,
}

// Convert `kad::Event` into `BehaviourEvent` for the `SwarmEvent`.
impl From<kad::Event> for BehaviourEvent {
    fn from(event: kad::Event) -> Self {
        BehaviourEvent::Kademlia(event)
    }
}

impl From<()> for BehaviourEvent {
    fn from(_value: ()) -> Self {
        BehaviourEvent::StreamNoop
    }    
}

/// Commands sent from `Client` to the `EventLoop`.
#[derive(Debug)]
enum Command {
    StartListening {
        addr: Multiaddr,
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
        sender: oneshot::Sender<Result<HashSet<PeerId>, AnyhowError>>,
    },
    // -----------------------
    // Stream-based file request:
    RequestFile {
        peer: PeerId,
        file_name: String,
        sender: oneshot::Sender<Result<Vec<u8>, AnyhowError>>,
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
