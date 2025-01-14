
pub mod network;
pub mod chunkblake;

use std::{error::Error, path::PathBuf}; //,

use futures::{prelude::*, StreamExt};
//use tokio::io::{AsyncReadExt, AsyncWriteExt};
use libp2p::{core::Multiaddr, multiaddr::Protocol};
//use tokio::task::spawn;
use tracing_subscriber::EnvFilter;//{field::debug, };

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
                                match chunkblake::send_file_with_hash(stream, path).await {
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
                // Find providers for the file name on the DHT.
                let providers = network_client.get_providers(name.clone()).await?;
                println!("Providers for {}: {:?}", name, providers);

                if providers.is_empty() {
                    return Err(format!("Could not find provider for file {name}.").into());
                }

                // Try requesting the file from each provider until one succeeds.
                let requests = providers.into_iter().map(|p| {
                    let mut network_client = network_client.clone();
                    let name = name.clone();
                    async move { network_client.request_file(p, name).await }.boxed()
                });

                let file_content = futures::future::select_ok(requests)
                    .await
                    .map_err(|_| "None of the providers returned file.")?
                    .0;

                // Write the file to stdout (or handle it however you wish).
                let out_path = std::path::Path::new("output_test/").join(name);
                std::fs::create_dir_all("output_test")?;
                std::fs::write(&out_path, &file_content)?;
                println!("File written to {:?}", out_path);
                // std::io::stdout().write_all(&file_content)?;
            }
        }
        Ok(())}
    }