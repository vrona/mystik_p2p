pub mod autonomos;

use content_hashing::{blakecheck, get_storage_dir};
use db_access::get_midcid_pairs;
use tokio::time::Duration;
use std::{pin::Pin, error::Error, path::PathBuf, collections::HashMap};
use futures::{prelude::*, StreamExt};
use libp2p::{PeerId, core::Multiaddr, multiaddr::Protocol, rendezvous};
use tracing_subscriber::EnvFilter;
use std::io::{self, Write};
use tracing::{debug, error}; 
use autonomos::{Client, Event, EventLoop};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::compat::FuturesAsyncWriteCompatExt; // try to make compatibility between tokio and libp2p stream
use tokio_stream;
use futures::channel::mpsc;
use tokio::io::{BufReader, AsyncBufReadExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;


pub struct AutonomosManager {
    pub client: Client,
    pub event_stream: Pin<Box<dyn Stream<Item = Event> + Send>>,
    pub event_loop: Arc<Mutex<EventLoop>>,
    /// Address on which to listen (or `None` to listen on 0.0.0.0:0).
    pub listen_address: Option<Multiaddr>,
    /// Peer to dial (optional) => bootstrapping or relaying
    pub peer: Option<Multiaddr>,
    midcid_memory_register: Arc<Mutex<HashMap<String, (PathBuf, String)>>>,

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
        ) = autonomos::new(secret_seed).await?;
        
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

        // checking existence of persistent mid and cid data and loading (necessary when node has been switched off)
        let current_persistent_midcid = Self::access_the_db().await?;

        Ok(Self {
            client,
            event_stream: Box::pin(event_stream),
            event_loop,
            listen_address,
            peer,
            midcid_memory_register: Arc::new(Mutex::new(current_persistent_midcid)),
        })
    } 

    // checking existence of persistent mid and cid data, return an HashMap
    pub async fn access_the_db() -> Result<HashMap::<String, (PathBuf, String)>, Box<dyn Error>> {
        // pub const STORAGE_DIR: &str = "./data/content/";
        
        let tmp_constant_path = get_storage_dir();

        let mut file_registry = HashMap::<String, (PathBuf, String)>::new();

        let pairs = get_midcid_pairs().await?;
        
        // if pairs are empty just return empty hashmap        
        if pairs.is_empty() {
            return Ok(file_registry);
        }
        
        for (mid, cid) in &pairs {
            let path = format!("{}{}.bin", tmp_constant_path, cid);
            let path_cid = PathBuf::from(path);
            println!("MID: {} -> CID: {}", mid, cid);
            file_registry.insert(mid.to_string(), (path_cid, cid.to_string()));
        }
        
        //tracing::debug!("\nAccess DB MID: {}\nCID: {}", midhex, cidhex);
        Ok(file_registry)
    }

    // Updating the HashMap of midcid to let Inbound Request been fulfilled
    pub async fn register_midcid_memory(&mut self, mid_as_hex: String, path_dir: PathBuf, cid_as_hex: String) -> Result<(), Box<dyn Error>>{
       
        let path = format!("{}{}.bin", path_dir.display(), cid_as_hex);
        let path_cid = PathBuf::from(path);
        let mut mirroring_midcid = self.midcid_memory_register.lock().await;
        mirroring_midcid.insert(mid_as_hex, (path_cid, cid_as_hex));
       
        Ok(())
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


    pub async fn launch_inbound_request_task(&mut self) -> Result<(), Box<dyn Error>> {
        let midcid_memory_register_clone = self.midcid_memory_register.clone();

        // Take ownership of the current event stream and replace it with an empty stream.
        let mut event_stream = std::mem::replace(&mut self.event_stream, Box::pin(futures::stream::empty()));

        // Spawn a local task to process inbound file request events directly.
        tokio::spawn(async move {
            tracing::debug!("Waiting for inbound file requests...");
            while let Some(event) = event_stream.next().await {
                match event {
                    autonomos::Event::InboundFileRequest { peer, file_name, stream } => {
                        tracing::debug!("Received inbound file request for: {}", file_name);

                        let midcid_memory_registry = midcid_memory_register_clone.lock().await;

                        if let Some((path, cid)) = midcid_memory_registry.get(&file_name) {
                            let path_cid = path.clone();
                            let local_cid = cid.clone();
                            let file_name_clone = file_name.clone();

                            // Use the stream directly.
                            let (_reader, writer) = stream.split();
                            let writer = writer.compat_write();

                            tokio::spawn(async move {
                                tracing::debug!("Handling file request from {:?} for file '{}'", peer, file_name_clone);

                                if let Err(e) = blakecheck::send_file_with_hash(writer, &path_cid, &local_cid).await {
                                    tracing::debug!("Error sending file: {} look path: {}", e, path_cid.display());
                                }
                            });
                        } else {
                            tracing::debug!("Peer {:?} requested unknown file {}", peer, file_name);
                        }
                    }
                    _ => {} // Optionally handle other events
                }
            }
        });

        Ok(())
    }


    
    pub async fn get_file(&mut self, name: String) -> Result<String, Box<dyn Error >>{

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
                async move { 
                   let result = client_clone.request_file(peer, name).await;
                    match &result {
                        Ok((file_bytes, cid)) => {
                            tracing::debug!("Provider {} succeeded with CID: {}", peer, cid);
                        }
                        Err(e) => {
                            tracing::debug!("Provider {} failed: {:?}", peer, e);
                        }
                    }
                    result
                }
                .boxed()
        });

        // select_ok returns a tuple (Vec<u8>, String) where .0 is the successful result.
        let (result, _remaining) = futures::future::select_ok(requests)
            .await
            .map_err(|_| "None of the providers returned file.")?;
        
        let (_file_bytes, cid) = result;
        
        Ok(cid)
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
