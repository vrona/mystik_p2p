pub mod node_regular;
pub mod node_rendezvous;
pub mod blakecheck;

use futures::{prelude::*, StreamExt};
use libp2p::{core::Multiaddr, multiaddr::Protocol, PeerId, rendezvous};
use std::{
    error::Error,
    pin::Pin,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::Mutex;
use tokio::time::Duration;
use tracing_subscriber::EnvFilter;

/// Mode selection – can be chosen at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeMode {
    Rendezvous,
    Regular,
}

/// NodeVariant wraps the three core components returned by the node_starter:
/// - client: the API used to issue commands,
/// - event_stream: a stream of events (e.g. inbound file requests),
/// - event_loop: the network event loop (which you must spawn).
pub enum NodeVariant {
    Rendezvous {
        client: node_rendezvous::Client,
        event_stream: Pin<Box<dyn Stream<Item = node_rendezvous::Event> + Send>>,
        event_loop: node_rendezvous::EventLoop,
    },
    Regular {
        client: node_regular::Client,
        event_stream: Pin<Box<dyn Stream<Item = node_regular::Event> + Send>>,
        event_loop: node_regular::EventLoop,
    },
}

/// node_starter creates the node components based on the given mode.
pub async fn node_starter(
    secret_seed_key: Option<u8>,
    mode: NodeMode,
) -> Result<NodeVariant, Box<dyn Error>> {
    match mode {
        NodeMode::Rendezvous => {
            let (client, event_stream, event_loop) =
                node_rendezvous::new(secret_seed_key).await?;
            Ok(NodeVariant::Rendezvous {
                client,
                event_stream: Box::pin(event_stream),
                event_loop,
            })
        }
        NodeMode::Regular => {
            let (client, event_stream, event_loop) =
                node_regular::new(secret_seed_key).await?;
            Ok(NodeVariant::Regular {
                client,
                event_stream: Box::pin(event_stream),
                event_loop,
            })
        }
    }
}

/// NodeManager provides higher-level functionality:
/// - It wraps the client, event loop and event stream.
/// - It listens on a specified multiaddr (or defaults to /ip4/0.0.0.0/udp/0/quic-v1).
/// - It optionally dials a peer (for bootstrapping or direct connection).
/// - It implements methods for spreading, providing, and retrieving files.
pub enum NodeManager {
    Rendezvous {
        client: node_rendezvous::Client,
        event_stream: Pin<Box<dyn Stream<Item = node_rendezvous::Event> + Send>>,
        event_loop: Arc<Mutex<node_rendezvous::EventLoop>>,
        /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
        listen_address: Option<Multiaddr>,
        /// Peer to dial (optional)
        peer: Option<Multiaddr>,
    },
    Regular {
        client: node_regular::Client,
        event_stream: Pin<Box<dyn Stream<Item = node_regular::Event> + Send>>,
        event_loop: Arc<Mutex<node_regular::EventLoop>>,
        /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
        listen_address: Option<Multiaddr>,
        /// Peer to dial (optional)
        peer: Option<Multiaddr>,
    },
}

impl NodeManager {
    /// Creates a new node. Note that you now must specify the mode.
    pub async fn new(
        secret_seed: Option<u8>,
        mode: NodeMode,
        listen_address: Option<Multiaddr>,
        peer: Option<Multiaddr>,
    ) -> Result<Self, Box<dyn Error>> {
        // Initialize logging.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();

        let node_variant = node_starter(secret_seed, mode.clone()).await?;
        match node_variant {
            NodeVariant::Rendezvous {
                mut client,
                event_stream,
                event_loop,
            } => {
                let event_loop = Arc::new(Mutex::new(event_loop));
                // Spawn the event loop in the background.
                {
                    let event_loop_clone = event_loop.clone();
                    let local_peer_id = event_loop_clone.lock().await.local_peer_id();
                    println!("Local PeerId: {}", local_peer_id);
                    tokio::spawn(async move {
                        event_loop_clone.lock().await.run().await;
                    });
                }
                // Listen on the provided address or fallback to default.
                match &listen_address {
                    Some(addr) => {
                        client
                            .start_listening(addr.clone())
                            .await
                            .expect("Listening should not fail.");
                    }
                    None => {
                        client
                            .start_listening("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)
                            .await
                            .expect("Listening should not fail.");
                    }
                }
                // Dial a peer if specified.
                if let Some(addr) = &peer {
                    let mut addr_iter = addr.iter();
                    if let Some(Protocol::P2p(peer_id)) = addr_iter.last() {
                        client.dial(peer_id, addr.clone()).await?;
                    } else {
                        return Err("Expected peer multiaddr to contain a PeerId.".into());
                    }
                }
                Ok(NodeManager::Rendezvous {
                    client,
                    event_stream: Box::pin(event_stream),
                    event_loop,
                    listen_address,
                    peer,
                })
            }
            NodeVariant::Regular {
                mut client,
                event_stream,
                event_loop,
            } => {
                let event_loop = Arc::new(Mutex::new(event_loop));
                {
                    let event_loop_clone = event_loop.clone();
                    let local_peer_id = event_loop_clone.lock().await.local_peer_id();
                    println!("Local PeerId: {}", local_peer_id);
                    tokio::spawn(async move {
                        event_loop_clone.lock().await.run().await;
                    });
                }
                match &listen_address {
                    Some(addr) => {
                        client
                            .start_listening(addr.clone())
                            .await
                            .expect("Listening should not fail.");
                    }
                    None => {
                        client
                            .start_listening("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)
                            .await
                            .expect("Listening should not fail.");
                    }
                }
                if let Some(addr) = &peer {
                    if let Some(Protocol::P2p(peer_id)) = addr.iter().last() {
                        client.dial(peer_id, addr.clone()).await?;
                    } else {
                        return Err("Expected peer multiaddr to contain a PeerId.".into());
                    }
                }
                Ok(NodeManager::Regular {
                    client,
                    event_stream: Box::pin(event_stream),
                    event_loop,
                    listen_address,
                    peer,
                })
            }
        }
    }

    /// Advertise self as a provider of `name`.
    pub async fn spread_ntwk(&mut self, name: String) -> Result<(), Box<dyn Error>> {
        match self {
            NodeManager::Regular { client, .. } => {
                client.start_providing(name.clone()).await?;
                tracing::debug!("Successfully started providing '{}'.", name);
            }
            NodeManager::Rendezvous { client, .. } => {
                client.start_providing(name.clone()).await?;
                tracing::debug!("Successfully started providing '{}'.", name);
            }
        }
        Ok(())
    }

    /// Loop and respond to inbound file requests matching `name`.
    pub async fn provide_file(&mut self, path: PathBuf, name: String) -> Result<(), Box<dyn Error>> {
        loop {
            tracing::debug!("Waiting for inbound file requests...");
            match self {
                NodeManager::Regular { event_stream, .. } => {
                    if let Some(event) = event_stream.as_mut().next().await {
                        match event {
                            node_regular::Event::InboundFileRequest { peer, file_name, stream } => {
                                tracing::debug!("Regular node: request for file '{}' from peer {}", file_name, peer);
                                if file_name == name {
                                    tracing::debug!("Match found. Sending file '{}'...", name);
                                    match blakecheck::send_file_with_hash(stream, &path).await {
                                        Ok(()) => {
                                            tracing::debug!("Sent file {} to peer {:?}", name, peer);
                                        }
                                        Err(e) => {
                                            tracing::debug!("Error sending file: {}", e);
                                        }
                                    }
                                } else {
                                    tracing::debug!("Peer {:?} requested unknown file '{}'", peer, file_name);
                                }
                            }
                        }
                    }
                }
                NodeManager::Rendezvous { event_stream, .. } => {
                    if let Some(event) = event_stream.as_mut().next().await {
                        match event {
                            node_rendezvous::Event::InboundFileRequest { peer, file_name, stream } => {
                                tracing::debug!("Rendezvous node: request for file '{}' from peer {}", file_name, peer);
                                if file_name == name {
                                    tracing::debug!("Match found. Sending file '{}'...", name);
                                    match blakecheck::send_file_with_hash(stream, &path).await {
                                        Ok(()) => {
                                            tracing::debug!("Sent file {} to peer {:?}", name, peer);
                                        }
                                        Err(e) => {
                                            tracing::debug!("Error sending file: {}", e);
                                        }
                                    }
                                } else {
                                    tracing::debug!("Peer {:?} requested unknown file '{}'", peer, file_name);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Retrieve a file by its `name`. It looks up a provider, dials the peer and then requests the file.
    pub async fn get_file(&mut self, name: String) -> Result<(), Box<dyn Error>> {
        // Determine the target peer multiaddr.
        let peer_addr = match self {
            NodeManager::Regular { client, event_loop, peer, .. } => {
                if let Some(addr) = peer {
                    addr.clone()
                } else {
                    let mut attempts = 0;
                    let mut provider_record: Option<Multiaddr> = None;
                    while attempts < 5 {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let providers = client.get_providers(name.clone()).await?;
                        println!("Attempt {}: Providers for {}: {:?}", attempts, name, providers);
                        if !providers.is_empty() {
                            let provider_peer_id = *providers.iter().next().unwrap();
                            println!("Found provider PeerId: {:?}", provider_peer_id);
                            let known_addrs = event_loop.lock().await.get_peer_addresses(&provider_peer_id);
                            if let Some(addr) = known_addrs.first() {
                                provider_record =
                                    Some(addr.clone().with(Protocol::P2p(provider_peer_id)));
                                break;
                            } else {
                                println!("No known addresses for provider {:?}; retrying...", provider_peer_id);
                            }
                        } else {
                            println!("No providers found on attempt {}.", attempts);
                        }
                    }
                    provider_record.ok_or_else(|| format!("Could not find provider for file {}", name))?
                }
            }
            NodeManager::Rendezvous { client, event_loop, peer, .. } => {
                if let Some(addr) = peer {
                    addr.clone()
                } else {
                    let mut attempts = 0;
                    let mut provider_record: Option<Multiaddr> = None;
                    while attempts < 5 {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let providers = client.get_providers(name.clone()).await?;
                        println!("Attempt {}: Providers for {}: {:?}", attempts, name, providers);
                        if !providers.is_empty() {
                            let provider_peer_id = *providers.iter().next().unwrap();
                            println!("Found provider PeerId: {:?}", provider_peer_id);
                            let known_addrs = event_loop.lock().await.get_peer_addresses(&provider_peer_id);
                            if let Some(addr) = known_addrs.first() {
                                provider_record =
                                    Some(addr.clone().with(Protocol::P2p(provider_peer_id)));
                                break;
                            } else {
                                println!("No known addresses for provider {:?}; retrying...", provider_peer_id);
                            }
                        } else {
                            println!("No providers found on attempt {}.", attempts);
                        }
                    }
                    provider_record.ok_or_else(|| format!("Could not find provider for file {}", name))?
                }
            }
        };

        // Extract the PeerId from the multiaddr.
        let peer_id = if let Some(Protocol::P2p(peer_id)) = peer_addr.iter().last() {
            peer_id
        } else {
            return Err("Determined peer address does not contain a PeerId.".into());
        };

        // Dial the peer and then request the file.
        match self {
            NodeManager::Regular { client, .. } => {
                client.dial(peer_id, peer_addr.clone()).await?;
                tokio::time::sleep(Duration::from_secs(3)).await;
                let providers = client.get_providers(name.clone()).await?;
                let requests = providers.into_iter().map(|p| {
                    let mut client_clone = client.clone();
                    let name = name.clone();
                    async move { client_clone.request_file(p, name).await }.boxed()
                });
                let _ = futures::future::select_ok(requests)
                    .await
                    .map_err(|_| "None of the providers returned file.")?
                    .0;
            }
            NodeManager::Rendezvous { client, .. } => {
                client.dial(peer_id, peer_addr.clone()).await?;
                tokio::time::sleep(Duration::from_secs(3)).await;
                let providers = client.get_providers(name.clone()).await?;
                let requests = providers.into_iter().map(|p| {
                    let mut client_clone = client.clone();
                    let name = name.clone();
                    async move { client_clone.request_file(p, name).await }.boxed()
                });
                let _ = futures::future::select_ok(requests)
                    .await
                    .map_err(|_| "None of the providers returned file.")?
                    .0;
            }
        }
        Ok(())
    }

    /// Register this node with a rendezvous server.
    /// (Only available for Rendezvous nodes.)
    pub async fn register_rendezvous(
        &mut self,
        rendezvous_addr: Multiaddr,
        rendezvous_peer: PeerId,
        namespace: String,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            NodeManager::Rendezvous { client, event_loop, .. } => {
                let ns = rendezvous::Namespace::new(namespace)?;
                client.dial(rendezvous_peer, rendezvous_addr.clone()).await?;
                event_loop
                    .lock()
                    .await
                    .swarm_mut()
                    .behaviour_mut()
                    .rendezvous
                    .register(ns, rendezvous_peer, None)
                    .map_err(|e| format!("Failed to register with rendezvous server: {}", e))?;
                Ok(())
            }
            NodeManager::Regular { .. } => {
                Err("register_rendezvous can only be used with a Rendezvous node".into())
            }
        }
    }

    /// Discover peers via a rendezvous server.
    /// (Only available for Rendezvous nodes.)
    pub async fn discover_rendezvous(
        &mut self,
        namespace: String,
        rendezvous_peer: PeerId,
    ) -> Result<(), Box<dyn Error>> {
        match self {
            NodeManager::Rendezvous { event_loop, .. } => {
                let ns = rendezvous::Namespace::new(namespace)?;
                event_loop
                    .lock()
                    .await
                    .swarm_mut()
                    .behaviour_mut()
                    .rendezvous
                    .discover(Some(ns), None, None, rendezvous_peer);
                Ok(())
            }
            NodeManager::Regular { .. } => {
                Err("discover_rendezvous can only be used with a Rendezvous node".into())
            }
        }
    }
}
