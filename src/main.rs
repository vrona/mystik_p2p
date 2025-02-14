// launch_node.rs

use std::error::Error;
use std::io::{self, BufRead};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::sync::Mutex;
use libp2p::{Multiaddr, multiaddr::Protocol, PeerId};
use libp2p::futures::future::pending;
use tracing_subscriber::EnvFilter;

// Import your network module's public interface.
use file_sharing::view_network;
use file_sharing::{Client, Event, EventLoop};

// Import your node command functions. Adjust the path as needed.
use nodecommand; // assumes nodecommand::store_file and nodecommand::get_cid_file exist


// Suppose `local_peer_id` is the PeerId of the current node.
async fn dial_bootstrap_if_needed(client: &mut Client, local_peer_id: &PeerId) -> Result<(), Box<dyn Error>> {
    // Try each bootstrap address (you might support multiple).
    for var in &["BOOTSTRAP_ADDR_1", "BOOTSTRAP_ADDR_2"] {
        if let Ok(addr_str) = std::env::var(var) {
            let addr: Multiaddr = addr_str.parse()?;
            if let Some(Protocol::P2p(bootstrap_peer_id)) = addr.iter().last() {
                if bootstrap_peer_id != *local_peer_id {
                    println!("Dialing bootstrap peer from {}: {}", var, addr);
                    // Note: since this is a synchronous helper, you'll need to await in an async context.
                    // For example, in your async main:
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    // 1. Initialize the network.
    // Here we use `None` for the secret_key_seed (i.e. generate a random identity).
    let (mut client, mut _event_stream, network_event_loop) = view_network(None).await?;
    let local_peer_id = network_event_loop.local_peer_id();
    println!("Local PeerId: {}", local_peer_id);

    // Wrap the event loop in an Arc<Mutex<_>> so it can be shared.
    let network_event_loop = Arc::new(Mutex::new(network_event_loop));

    // Spawn the event loop in a background task.
    {
        let event_loop_clone = network_event_loop.clone();
        tokio::spawn(async move {
            event_loop_clone.lock().await.run().await;
        });
    }

    // Start listening on a default address.
    let listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
    client.start_listening(listen_addr).await?;
    // Wait a moment for listening to be established.
    sleep(Duration::from_secs(5)).await;
    println!("Node launched and connected to the network.");

    // 2.1 Explicit Bootstrapping:
    dial_bootstrap_if_needed(&mut client, &local_peer_id).await;

    // 2.2 Dynamic bootstrap() method on Kad here if desired.)
    // For example, if you have access to the event loop:
    {
        let mut event_loop_lock = network_event_loop.lock().await;
        match event_loop_lock.bootstrap() {
            Ok(query_id) => println!("Bootstrap initiated: {:?}", query_id),
            Err(e) => println!("Bootstrap failed: {:?}", e),
        }
    }

    // Keep the node running.
    pending::<()>().await;
    Ok(())
}


 // 3. Unified Interface for Node Commands:
    // Provide a simple command-line loop that allows the user (or an AI agent)
    // to call high-level functions such as store_file() and get_cid_file().
    
    //-_-_-_-_-_-_-_-_-_
    // println!("Enter command: (store <filepath> | get <cid> | exit)");
    // let stdin = io::stdin();
    // for line in stdin.lock().lines() {
    //     let input = line?;
    //     let parts: Vec<&str> = input.trim().split_whitespace().collect();
    //     if parts.is_empty() {
    //         continue;
    //     }
    //     match parts[0] {
    //         "store" if parts.len() == 2 => {
    //             let filepath = parts[1];
    //             match nodecommand::store_file(filepath.to_string()).await {
    //                 Ok(cid) => println!("File stored successfully, CID: {}", cid),
    //                 Err(e) => eprintln!("Error storing file: {}", e),
    //             }
    //         }
    //         "get" if parts.len() == 2 => {
    //             let cid = parts[1];
    //             match nodecommand::get_cid_file(cid.to_string()).await {
    //                 Ok(()) => println!("File retrieved successfully."),
    //                 Err(e) => eprintln!("Error retrieving file: {}", e),
    //             }
    //         }
    //         "exit" => break,
    //         _ => println!("Unknown command. Usage: store <filepath> | get <cid> | exit"),
    //     }
    //     println!("Enter command: (store <filepath> | get <cid> | exit)");
    // }