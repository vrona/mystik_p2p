use std::env;
pub mod network;
pub mod blakecheck;
use futures::future::join_all;
use tokio::time::{Duration, sleep, timeout};
use std::{pin::Pin, error::Error, path::PathBuf};
use futures::{prelude::*, StreamExt};
use libp2p::{PeerId, core::Multiaddr, multiaddr::Protocol, kad};
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

        // Initialize logging.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();

        let (mut client, event_stream, network_event_loop) = network::new(secret_seed).await?;
        let event_loop = Arc::new(Mutex::new(network_event_loop));

         // Spawn the network in the background.
        {
            let event_loop_clone = event_loop.clone();
                let local_peer_id = event_loop_clone.lock().await.local_peer_id();
                println!("Local PeerId: {}", local_peer_id);

                // Explicit dial_bootstrap_if_needed.
                //let _ = Self::robust_bootstrap(&mut client, local_peer_id.clone(), event_loop.clone()).await?;

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

    // /// If that fails due to no known peers, dial known bootstrap addresses.
    // pub async fn robust_bootstrap(
    //     client: &mut Client,
    //     local_peer_id: PeerId,
    //     event_loop: Arc<Mutex<EventLoop>>, // might be used for further logic if needed
    // ) -> Result<(), Box<dyn Error>> {
    //     // Try built-in bootstrap first.
    //     match event_loop.lock().await.bootstrap() {
    //         Ok(query_id) => {
    //             println!("Bootstrap initiated with query_id: {:?}", query_id);
    //             return Ok(());
    //         }
    //         Err(kad::NoKnownPeers()) => {
    //             println!("Built-in bootstrap failed: No known peers. Proceeding to dial bootstrap addresses...");
    //         }
    //         Err(e) => return Err(e.into()),
    //     }

    //     // Collect bootstrap addresses from environment variables.
    //     let bootstrap_addrs: Vec<Multiaddr> = ["BOOTSTRAP_ADDR_1", "BOOTSTRAP_ADDR_2"]
    //         .iter()
    //         .filter_map(|var| env::var(var).ok())
    //         .filter_map(|addr_str| addr_str.parse().ok())
    //         .collect();

    //     println!("BOOTSTRAPS ADDR: {:?}", bootstrap_addrs);
    //     // Prepare dial tasks for bootstrap addresses.
    //     let dial_tasks: Vec<_> = bootstrap_addrs.into_iter().filter_map(|addr| {
    //         if let Some(Protocol::P2p(bootstrap_peer_id)) = addr.iter().last() {
    //             // Skip if it's our own ID.
    //             if bootstrap_peer_id == local_peer_id {
    //                 println!("Skipping bootstrap connection to self.");
    //                 return None;
    //             }
    //             println!("Preparing to dial bootstrap peer: {}", addr);
    //             let mut client_clone = client.clone();
    //             Some(async move {
    //                 let mut attempt = 0;
    //                 let max_attempts = 5;
    //                 let mut backoff = Duration::from_secs(1);
    //                 loop {
    //                     attempt += 1;
    //                     match timeout(Duration::from_secs(10), client_clone.dial(bootstrap_peer_id, addr.clone())).await {
    //                         Ok(Ok(())) => {
    //                             println!("Successfully connected to bootstrap peer {} on attempt {}", addr, attempt);
    //                             return Ok(());
    //                         }
    //                         Ok(Err(e)) => {
    //                             println!("Dial attempt {} to {} failed: {}", attempt, addr, e);
    //                         }
    //                         Err(_) => {
    //                             println!("Dial attempt {} to {} timed out", attempt, addr);
    //                         }
    //                     }
    //                     if attempt >= max_attempts {
    //                             println!("Giving up on bootstrap peer {} after {} attempts", addr, attempt);
    //                             return Err("Max dial attempts reached");
    //                     }
    //                     sleep(backoff).await;
    //                     backoff *= 2;
    //                 }
    //             })
    //         } else {
    //             println!("Skipping address without a /p2p/ component: {}", addr);
    //             None
    //         }
    //     }).collect();

    //     // If there are no external bootstrap addresses, that's acceptable for a bootstrap node.
    //     if dial_tasks.is_empty() {
    //         println!("No external bootstrap addresses to dial; running as sole bootstrap node.");
    //         return Ok(());
    //     }

    //     // Run all dial tasks concurrently.
    //     let results = join_all(dial_tasks).await;
    //     if results.iter().any(|res| res.is_ok()) {
    //         Ok(())
    //     } else {
    //         Err("Failed to dial any bootstrap peers.".into())
    //     }
    // }


    /// Dial bootstrap peers concurrently with retry logic.
    

    pub async  fn spread_ntwk(&mut self, name: String) -> Result<(), Box<dyn Error>> {
         // Advertise self as a provider of `name`.
        match self.client.start_providing(name.clone()).await {
            Ok(_) => {
                tracing::debug!("Successfully started providing '{}'.", name);
            }
            Err(e) => {
                tracing::debug!("Failed to start providing '{}': {}", name, e);
            }
        }
        Ok(())
    }

    pub async fn provide_file(&mut self, path: PathBuf, name: String) -> Result<(), Box<dyn Error>> {

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

            // Retry mechanism: attempt multiple times until we find a provider.
            let mut attempts = 0;
            let max_attempts = 5;
            let mut provider_record: Option<Multiaddr> = None;
            while attempts < max_attempts {
                attempts += 1;
            // Optionally wait a short time for DHT propagation.
            tokio::time::sleep(Duration::from_secs(3)).await;
            
            // Query providers for the file name.
            let providers = self.client.get_providers(name.clone()).await?;
            println!("Attempt {}: Providers for {}: {:?}", attempts, name, providers);
            
            if !providers.is_empty() {
                // Select the first provider.
                let provider_peer_id = *providers.iter().next().unwrap();
                println!("Found provider PeerId: {:?}", provider_peer_id);
                
                // Try to retrieve known addresses for that provider.
                let remote_addr: Option<Multiaddr> = {
                    let known_addrs: Vec<Multiaddr> = {
                        let mut event_loop_lock = self.event_loop.lock().await;
                        event_loop_lock.get_peer_addresses(&provider_peer_id)
                    };
                    known_addrs.first().map(|addr| addr.clone().with(Protocol::P2p(provider_peer_id)))
                };
                
                if let Some(addr) = remote_addr {
                    provider_record = Some(addr);
                    break;
                } else {
                    println!("No known addresses for provider {:?}; retrying...", provider_peer_id);
                }
            } else {
                println!("No providers found on attempt {}.", attempts);
            }
        }
        provider_record.ok_or_else(|| format!("Could not find provider for file {}", name))?
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
