// launch_node.rs
mod node_config; // Import the configuration module
use std::error::Error;
use dotenv::dotenv;
use std::{pin::Pin, env};
use std::io::{self, Write, BufRead};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::sync::Mutex;
use libp2p::{Multiaddr, multiaddr::Protocol, PeerId};
use libp2p::futures::future::pending;
use tracing_subscriber::EnvFilter;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

// Import your network module's public interface.
use file_sharing::NodeManager;

// Import your node command functions.
use nodecommand;



#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    // Load node configuration.
    let config = node_config::load_node_config()?;

    // Start listening on a default address.
    let listen_address: Option<Multiaddr> = config.listen_addr;

    // Peer
    let peer: Option<Multiaddr> = None;

    // Create launch Node
    let mut node_manager = NodeManager::new(config.secret_seed, listen_address, peer).await?;
    // Wait a moment for listening to be established.
    sleep(Duration::from_secs(5)).await;
    println!("Node launched and connected to the network.");

    
    // Optionally, perform dynamic bootstrapping.
    // {
    //     let shared_event_loop_clone = shared_event_loop.clone();
    //     tokio::spawn(async move {
    //         let mut event_loop_lock = shared_event_loop_clone.lock().await;
    //         match event_loop_lock.bootstrap() {
    //             Ok(query_id) => println!("Bootstrap initiated: {:?}", query_id),
    //             Err(e) => println!("Bootstrap failed: {:?}", e),
    //         }
    //     });
    // }

    // 3. Command-line interface.
    println!(
        "Enter command: (store <file_name> <file_type> <secret> <file_path>\
         | spread <mid (metadata id)>\
         | call <mid (metadata id)> <peer>\
         | get <file_name> <file_type> <secret> <private_key> <nonce>\
         | exit)"
    );

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let input = line?;
        let parts: Vec<&str> = input.trim().split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        match parts[0] {
            "store" if parts.len() == 5 => {
                let file_name = parts[1];
                let file_type = parts[2];
                let secret = parts[3];
                let file_path = parts[4];
                match nodecommand::store_file(
                    file_name,
                    file_type,
                    secret,
                    file_path
                )
                .await
                {
                    Ok((private_key, nonce, _)) => {
                        println!("🔍 private_key: {:?}", private_key);
                        println!("🔍 nonce: {:?}", nonce);
                    }
                    Err(e) => eprintln!("Error storing file: {}", e),
                }
            }
            "spread" => {
                if parts.len() == 2 {
                    let mid_as_hex = parts[1];
                    // let listen_addr = "/ip4/0.0.0.0/udp/0/quic-v1";
                    // let peer_bootstrap = "/ip4/127.0.0.1/udp/9001/quic-v1/p2p/12D3KooWMzg8FK5YW9SSov78gYudoeJxsWy4GbjMg7fA6JKv2X8T";
                    let path = nodecommand::local_path_to_cid(mid_as_hex).await?;
                    match node_manager.provide_file(path, mid_as_hex.to_string()).await
                    {
                        Ok(()) => println!("File transmitted successfully!"),
                        Err(e) => eprintln!("Error transmitting file: {}", e),
                    }
                }
            }
            "call" => {
                if parts.len() == 3 {
                    let mid_as_hex = parts[1];
                    let cid_to_hex = nodecommand::midhex_to_cidhex(mid_as_hex).await?;
                    match node_manager.get_file(cid_to_hex).await
                    {
                        Ok(()) => println!("File transmitted successfully!"),
                        Err(e) => eprintln!("Error transmitting file: {}", e),
                    }
                }
            }
            "get" => {
                println!("🔍 Raw Input: '{}'", input);
                let parts: Vec<&str> = input.trim().splitn(6, ' ').collect();
                if parts.len() < 6 {
                    eprintln!("Error: 'get' command requires at least 6 arguments but got {}", parts.len());
                    continue;
                }
                let file_name = parts[1];
                let file_type = parts[2];
                let secret = parts[3];
                let private_key = parts[4];
                let nonce = parts[5];
                match nodecommand::get_cid_file(file_name, file_type, secret).await {
                    Ok(returned_cid) => {
                        println!("Debug: Returned CID: {:?}", returned_cid);
                        let decrypt_path = format!(
                            "/Volumes/vrona_SSD/rust_premiere/backup_old_mystik>p2p/data/dec/{}{}",
                            file_name, file_type
                        );
                        nodecommand::get_decrypt_file(private_key, nonce, &returned_cid, &decrypt_path)
                            .await?;
                        println!("File retrieved successfully.")
                    }
                    Err(e) => eprintln!("Error retrieving file: {}", e),
                }
            }
            "exit" => break,
            _ => println!("Unknown command. Usage: store <file_name> <file_type> <secret> <file_path>\
                           | spread <mid (metadata id)>\
                           | call <mid (metadata id)> <peer>\
                           | get <file_name> <file_type> <secret> <private_key> <nonce>\
                           | exit"),
        }
        println!("Enter command: (store <file_name> <file_type> <secret> <file_path>\
                   | spread <mid (metadata id)>\
                   | call <mid (metadata id)> <peer>\
                   | get <file_name> <file_type> <secret> <private_key> <nonce>\
                   | exit)");
    }

    // Keep the node running.
    pending::<()>().await;
    Ok(())
}
