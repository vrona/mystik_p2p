
pub mod network;
pub mod blakecheck;
use tokio::time::Duration;
use std::{error::Error, path::PathBuf};
use futures::{prelude::*, StreamExt};
use libp2p::{core::Multiaddr, multiaddr::Protocol};
use tracing_subscriber::EnvFilter;
use std::io::{self, Write};
pub use crate::network::{Client, Event, EventLoop};

#[derive(Debug)]
pub struct RunOption {
    /// Secret key seed for deterministic peer ID (or `None` for random).
    pub secret_key_seed: Option<u8>,
    /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
    pub listen_address: Option<Multiaddr>,
    /// Peer to dial (optional).
    pub peer: Option<Multiaddr>,
    /// Argument "Provide" or "Get" (optional).
    pub argument: ArgProvOrGet,
}

#[derive(Debug)]
pub enum ArgProvOrGet {
    /// Provide path to a file with a given name.
    Provide{path: PathBuf, name: String},

    /// Get a file with a given name.
    Get{ name: String },
}

impl RunOption {
    pub async  fn run(&self) -> Result<(), Box<dyn Error>> {
        let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

        // Create the network, obtaining a client, an event stream, and a network loop.
        let (mut network_client, mut network_events, mut network_event_loop) =
            network::new(self.secret_key_seed).await?;

        // Spawn the network in the background.
        tokio::spawn(async move {
            network_event_loop.run().await;
        });

        // Listen on the user-provided address, or fallback to /ip4/0.0.0.0/udp/0/quic-v1.
        match &self.listen_address {
            Some(addr) => {
                network_client
                    .start_listening(addr.clone())
                    .await
                    .expect("Listening not to fail.");
            }
            None => {
                network_client
                    .start_listening("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)
                    .await
                    .expect("Listening not to fail.");
            }
        }

        // Dial a peer if specified.
        if let Some(addr) = &self.peer {
            // Extract the PeerId from the multiaddr (the /p2p/ component).
            let Some(Protocol::P2p(peer_id)) = addr.iter().last() else {
                return Err("Expect peer multiaddr to contain peer ID.".into());
            };
            network_client
                .dial(peer_id, addr.clone())
                .await
                .expect("Dial to succeed");
        }

        // Handle either "Provide" or "Get".
        match &self.argument {
            ArgProvOrGet::Provide { path, name } => {
                // Advertise self as a provider of `name`.
                match network_client.start_providing(name.clone()).await {
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
                    match network_events.next().await {
                        Some(network::Event::InboundFileRequest { peer, file_name, stream }) => {
                            // 1. check if the requested file name matches our `name`.
                            tracing::debug!("Lib.rs checks: {} {}", file_name, name);
                            if file_name == *name {
                                tracing::debug!("Inbound request from {peer} matched our file '{file_name}'. Calling send_file_with_hash now!");
                                 // 1) We want to send that file in chunks with BLAKE3 verification.
                                // So we call chunkblake::send_file_with_hash:
                                match blakecheck::send_file_with_hash(stream, path).await {
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
                            tracing::warn!("Unexpected event: {:?}", other);
                        }
                    }
                }
            }
            ArgProvOrGet::Get { name } => {
                
                let peer_addr = if let Some(addr) = &self.peer {
                    addr.clone()
                } else {
                    // Optionally wait a short time for DHT propagation.
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    // Find providers for the file name on the DHT.
                    let providers = network_client.get_providers(name.clone()).await?;
                    println!("Providers for {}: {:?}", name, providers);

                    if providers.is_empty() {
                        return Err(format!("Could not find provider for file {name}.").into());
                    }

                    let provider_peer_id = *providers.iter().next().unwrap();
                    println!("Automatically found provider PeerId: {:?}", provider_peer_id);

                    let base_addr: Multiaddr = "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?;
                    let remote_addr = base_addr.with(Protocol::P2p(provider_peer_id));
                    println!("Constructed multiaddr: {:?}", remote_addr);
                    remote_addr
                    };

                    // Now, extract the PeerId from the full multiaddr.
                    let Some(Protocol::P2p(peer_id)) = peer_addr.iter().last() else {
                        return Err("Determined peer address does not contain a PeerId.".into());
                    };

                    // Dial the peer
                    network_client.dial(peer_id, peer_addr.clone()).await?;

                    // Once connected, proceed to request the file.
                    let requests = network_client
                        .get_providers(name.clone())
                        .await?
                        .into_iter()
                        .map(|p| {
                            let mut network_client = network_client.clone();
                            let name = name.clone();
                            async move { network_client.request_file(p, name).await }.boxed()
                        });
                    // OLD Working Try requesting the file from each provider until one succeeds.
                    // let requests = providers.into_iter().map(|p| {
                    //     let mut network_client = network_client.clone();
                    //     let name = name.clone();
                    //     async move { network_client.request_file(p, name).await }.boxed()
                    // });

                let _ = futures::future::select_ok(requests)
                    .await
                    .map_err(|_| "None of the providers returned file.")?
                    .0;
            }
        }
        Ok(())}
    }

pub async fn view_network(secret_seed_key: Option<u8>) -> Result<(Client, impl Stream<Item = Event>, EventLoop), Box<dyn Error>> {
    crate::network::new(secret_seed_key)
        .await
        .map_err(Into::into)

}