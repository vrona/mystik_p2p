use std::env;
pub mod network;
pub mod blakecheck;
use tokio::time::Duration;
use std::{pin::Pin, error::Error, path::PathBuf};
use futures::{prelude::*, StreamExt};
use libp2p::{PeerId, core::Multiaddr, multiaddr::Protocol};
use tracing_subscriber::EnvFilter;
use std::io::{self, Write};
pub use crate::network::{Client, Event, EventLoop};

use std::sync::Arc;
use tokio::sync::Mutex;

// Create a Node (client, event_stream, network loop)
pub async fn node_starter(secret_seed_key: Option<u8>) -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    crate::network::new(secret_seed_key)
        .await
        .map_err(Into::into)
}

pub struct NodeManager {
    pub client: Client,
    pub event_stream: Pin<Box<dyn Stream<Item = Event> + Send>>,
    pub event_loop: Arc<Mutex<EventLoop>>,
    /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
    pub listen_address: Option<Multiaddr>,
    /// Peer to dial (optional).
    pub peer: Option<Multiaddr>,
}

impl NodeManager{

    pub async fn new(secret_seed: Option<u8>, listen_address:Option<Multiaddr>, peer:Option<Multiaddr>) -> Result<Self, Box<dyn Error>> {
        let (mut client, event_stream, network_event_loop) = network::new(secret_seed).await?;
        let event_loop = Arc::new(Mutex::new(network_event_loop));

         // Spawn the network in the background.
        {
            let event_loop_clone = event_loop.clone();
                let local_peer_id = event_loop_clone.lock().await.local_peer_id();
                println!("Local PeerId: {}", local_peer_id);

                // Explicit bootstrapping.
                let _ = Self::dial_bootstrap_if_needed(client.clone(), local_peer_id.clone()).await;

                tokio::spawn(async move {
                event_loop_clone.lock().await.run().await;
            });
        }

        // Listen on the user-provided address, or fallback to /ip4/0.0.0.0/udp/0/quic-v1.
        match &listen_address {
            Some(addr) => {
                client
                    .start_listening(addr.clone())
                    .await
                    .expect("Listening not to fail.");
            }
            None => {
                client
                    .start_listening("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)
                    .await
                    .expect("Listening not to fail.");
            }
        }

        // Dial a peer if specified.
        if let Some(addr) = &peer {
            // Extract the PeerId from the multiaddr (the /p2p/ component).
            let Some(Protocol::P2p(peer_id)) = addr.iter().last() else {
                return Err("Expect peer multiaddr to contain peer ID.".into());
            };
            client
                .dial(peer_id, addr.clone())
                .await
                .expect("Dial to succeed");
        }



        Ok(Self {
            client,
            event_stream: Box::pin(event_stream),
            event_loop,
            listen_address,
            peer,
        })
    }

    /// Helper: Dial bootstrap peers if needed.
    async fn dial_bootstrap_if_needed(
        mut client: Client,
        local_peer_id: PeerId,
    ) -> Result<(), Box<dyn Error>> {
        for var in &["BOOTSTRAP_ADDR_1", "BOOTSTRAP_ADDR_2"] {
            if let Ok(addr_str) = env::var(var) {
                let addr: Multiaddr = addr_str.parse()?;
                if let Some(Protocol::P2p(bootstrap_peer_id)) = addr.iter().last() {
                    if bootstrap_peer_id != local_peer_id {
                        println!("Dialing bootstrap peer from {}: {}", var, addr);
                        client.dial(bootstrap_peer_id, addr).await?;
                    } else {
                        println!("Skipping bootstrap connection to self (from {}).", var);
                    }
                } else {
                    eprintln!("Bootstrap address {} does not contain a /p2p/ component.", addr_str);
                }
            }
        }
        Ok(())
    }

    pub async fn provide_file(&mut self, path: PathBuf, name: String) -> Result<(), Box<dyn Error>> {

        // Advertise self as a provider of `name`.
        match self.client.start_providing(name.clone()).await {
            Ok(_) => {
                tracing::debug!("Successfully started providing '{}'.", name);
            }
            Err(e) => {
                tracing::debug!("Failed to start providing '{}': {}", name, e);
            }
        }

        // Loop and respond to inbound file requests that match `name`.
        loop {
            tracing::debug!("Waiting for inbound file requests...");
            match self.event_stream.next().await {
                Some(network::Event::InboundFileRequest { peer, file_name, stream }) => {
                    // 1. check if the requested file name matches our `name`.
                    tracing::debug!("Lib.rs checks: {} {}", file_name, name);
                    if file_name == *name {
                        tracing::debug!("Inbound request from {peer} matched our file '{file_name}'. Calling send_file_with_hash now!");
                            // 1) We want to send that file in chunks with BLAKE3 verification.
                        // So we call chunkblake::send_file_with_hash:
                        match blakecheck::send_file_with_hash(stream, &path).await {
                            Ok(()) => {
                                tracing::debug!("Sent file {} to peer {:?}", name, peer);
                            }
                            Err(e) => {
                                tracing::debug!("Error sending file: {e}");
                            }
                        }
                    } else {
                    tracing::debug!("Peer {:?} requested unknown file {file_name}", peer);
                    // Possibly write some error bytes back, or do nothing
                    }
                }
                // For simplicity, skip unimplemented events. other => todo!("{:?}", other),
                other => {
                    tracing::warn!("Unexpected event_stream: {:?}", other);
                }
            }
        }
    }

    pub async fn get_file(&mut self, name: String) -> Result<(), Box<dyn Error >>{

        let peer_addr = if let Some(addr) = &self.peer {
            addr.clone()
        } else {
            // Optionally wait a short time for DHT propagation.
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Find providers for the file name on the DHT.
            let providers = self.client.get_providers(name.clone()).await?;
            println!("Providers for {}: {:?}", name, providers);

            if providers.is_empty() {
                return Err(format!("Could not find provider for file {name}.").into());
            }

            let provider_peer_id = *providers.iter().next().unwrap();
            println!("Automatically found provider PeerId: {:?}", provider_peer_id);

            // Use the shared event_stream loop to retrieve known addresses.
            let remote_addr: Multiaddr = {
                let known_addrs: Vec<Multiaddr> = {
                    // Lock the event_stream loop to get the addresses.
                    let mut event_loop_lock = self.event_loop.lock().await;
                    event_loop_lock.get_peer_addresses(&provider_peer_id)
                };
                
                // Use the first known address and append the /p2p/ component.
                known_addrs
                    .first()
                    .map(|addr| addr.clone().with(Protocol::P2p(provider_peer_id)))
                    .ok_or_else(|| Box::<dyn std::error::Error>::from("No known addresses for the provider."))?
            };
            remote_addr
        };

        // Now, extract the PeerId from the full multiaddr.
        let Some(Protocol::P2p(peer_id)) = peer_addr.iter().last() else {
            return Err("Determined peer address does not contain a PeerId.".into());
        };

        // Dial the peer
        self.client.dial(peer_id, peer_addr.clone()).await?;

        // Optionally wait a short time for the DHT to propagate provider records.
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Once connected, proceed to request the file.
        let requests = self.client
            .get_providers(name.clone())
            .await?
            .into_iter()
            .map(|p| {
                let mut client_clone = self.client.clone();
                let name = name.clone();
                async move { client_clone.request_file(p, name).await }.boxed()
            });
        // OLD Working Try requesting the file from each provider until one succeeds.
        // let requests = providers.into_iter().map(|p| {
        //     let mut self.client = self.client.clone();
        //     let name = name.clone();
        //     async move { self.client.request_file(p, name).await }.boxed()
        // });

        let _ = futures::future::select_ok(requests)
            .await
            .map_err(|_| "None of the providers returned file.")?
            .0;
        
        Ok(())
    }
}
