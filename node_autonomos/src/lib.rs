
pub mod node_regular;
use content_hashing::blakecheck;
use tokio::time::Duration;
use std::{pin::Pin, error::Error, path::PathBuf, collections::HashMap};
use futures::{prelude::*, StreamExt};
use libp2p::{PeerId, core::Multiaddr, multiaddr::Protocol, rendezvous};
use tracing_subscriber::EnvFilter;
use std::io::{self, Write};
use tracing::debug; 
use node_regular::{Client, Event, EventLoop};
use std::sync::Arc;
use tokio::sync::Mutex;

use metadata_mrkl::{MonoMetaTree, bytes_to_hex};

pub struct AutonomosManager {
    pub client: Client,
    pub event_stream: Pin<Box<dyn Stream<Item = Event> + Send>>,
    pub event_loop: Arc<Mutex<EventLoop>>,
    /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
    pub listen_address: Option<Multiaddr>,
    /// Peer to dial (optional) => bootstrapping or relaying
    pub peer: Option<Multiaddr>,

}


impl AutonomosManager{

    pub async fn new(secret_seed: Option<u8>, listen_address:Option<Multiaddr>, peer:Option<Multiaddr>) -> Result<Self, Box<dyn Error>> {

        // Initialize logging.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();

        let (
            mut client,
            event_stream,
            composite_event_loop,
        ) = node_regular::new(secret_seed).await?;
        
        // let event_loop = Arc::new(Mutex::new(composite_event_loop));

        // // Spawn the composite in the background.
        // {
        //     let event_loop_clone = event_loop.clone();
        //         tokio::spawn(async move {
        //         event_loop_clone.lock().await.run().await;
        //     });        
        // }

         // Extract the shared listen address handle from the event loop.
        // This field is an Arc<Mutex<Option<Multiaddr>>> that is updated by the event loop.
        let listen_addr_handle = composite_event_loop.actual_listen_addr.clone();

        // Spawn the event loop using its spawn() method.
        // This consumes the composite event loop and runs it in its own task.
               // Wrap the original event loop in an Arc so that other functions can later
        // send commands (if needed) via locking.
        // (Note: Direct mutable access to the swarm is discouraged; instead, use the client.)
        let event_loop = Arc::new(Mutex::new(composite_event_loop));

        let local_peer_id: PeerId = {
        let guard = event_loop.lock().await;
            (*guard).local_peer_id()
            };
        {
        let event_loop_clone = event_loop.clone();
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
                    .start_listening("/ip4/127.0.0.1/udp/0/quic-v1".parse()?)
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

         let addr_to_advertise = match &listen_address {
            Some(user_addr) => user_addr.clone(),
            None => {
                loop {
                    let maybe_addr = {
                        let guard = listen_addr_handle.lock().await;
                        (*guard).clone()
                    };
                    if let Some(addr) = maybe_addr {
                        // println!("Got ephemeral address: {}", addr);
                        break addr;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };

        // 8) Advertise on the DHT
        client.advertise(local_peer_id, addr_to_advertise).await?;

        Ok(Self {
            client,
            event_stream: Box::pin(event_stream),
            event_loop,
            listen_address,
            peer,
        })
    }    

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
                Some(node_regular::Event::InboundFileRequest { peer, file_name, stream }) => {
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

    pub async fn midkey_to_cidhex_as_name(mid_as_key: [u8;32])-> Result<String, Box<dyn Error>> {
        // init tree (Monotree) instance with the persistent database (rocksdb) and hasher (blake3)
        let mut tree = MonoMetaTree::new()?;

        // get the MID CID pair
        if let Some(bytes_of_cid) = tree.get(&mid_as_key)?{
            let cid_as_hex = bytes_to_hex::<32>(bytes_of_cid).await;

            println!("cid test midkey_to_cidhex_as_name: {}", cid_as_hex);
            return Ok(cid_as_hex)
        };

        Ok("Wrong MID or No CID founded".into())
    }

    pub async fn get_file(&mut self, mid_as_key: [u8;32]) -> Result<(), Box<dyn Error >>{

        let name = Self::midkey_to_cidhex_as_name(mid_as_key).await?;

        let peer_addr = if let Some(addr) = &self.peer {
            addr.clone()
        } else {

            tokio::time::sleep(Duration::from_secs(5)).await;
            
            let provider_map: HashMap<PeerId, Vec<Multiaddr>> = self.client.get_providers(name.clone()).await?;
            if provider_map.is_empty() {
                println!("No providers found for file {}.", name);
                return Err(format!("Could not find provider for file {}", name).into());
            }

            // Select the first provider that has at least one address.
            let (provider_peer_id, addrs) = provider_map.into_iter()
                .find(|(_, addrs)| !addrs.is_empty())
                .ok_or_else(|| format!("Could not find a provider with a known address for file {}", name))?;

            println!("Found provider PeerId: {:?}", provider_peer_id);
            println!("Known addresses: {:?}", addrs);

            // Pick the first address and attach the peer ID (if not already attached).
            let peer_addr = addrs.first()
                .map(|addr| {
                    if !addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
                        addr.clone().with(Protocol::P2p(provider_peer_id))
                    } else {
                        addr.clone()
                    }
                })
                .ok_or_else(|| format!("Provider {} has no addresses", provider_peer_id))?;
            
            
            peer_addr
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
            .map(|(peer, _addrs)| {
                let mut client_clone = self.client.clone();
                let name = name.clone();
                async move { client_clone.request_file(peer, name).await }.boxed()
            });

        let _ = futures::future::select_ok(requests)
            .await
            .map_err(|_| "None of the providers returned file.")?
            .0;
        
        Ok(())
    }

    /// Register this node with a rendezvous server.
    /// rendezvous_addr: multiaddr of the rendezvous server.
    /// rendezvous_peer: PeerId of the rendezvous server.
    /// namespace: the namespace in which to register.
    pub async fn register_rendezvous(&mut self, rendezvous_addr: Multiaddr, rendezvous_peer: PeerId, namespace: &'static str) -> Result<(), Box<dyn Error>> {
        
        debug!("register_rendezvous fn has been called");
        let _ = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .try_init();

        //self.event_loop.lock().await.swarm_mut().add_external_address(rendezvous_addr.clone());
        println!("rendezvous_addr: {}", rendezvous_addr);

        // Dial the rendezvous server if not already connected.
        self.client.register(rendezvous_peer, rendezvous_addr.clone(), namespace).await?;

        // Once connected, register.

        Ok(())
    }

    // TO DEL Discover peers via a rendezvous server.
    // namespace: the namespace to query.
    // rendezvous_peer: PeerId of the rendezvous server.
    // pub async fn discover_rendezvous(&mut self, namespace: &'static str, rendezvous_peer: PeerId) -> Result<(), Box<dyn Error>> {
    //     let ns = rendezvous::Namespace::from_static(namespace);
    //     self.event_loop.lock().await.swarm_mut().behaviour_mut().rendezvous.discover(Some(ns), None, None, rendezvous_peer);
    //     Ok(())
    // }

}
