// launch_node.rs
mod node_config; // Import the configuration module
use std::error::Error;
use std::io::{self, BufRead};
use tokio::time::{sleep, Duration};
use libp2p::Multiaddr;
use libp2p::futures::future::pending;
use tracing_subscriber::EnvFilter; 
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

    // Command-line interface.
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
                    let path = nodecommand::local_path_to_cid(mid_as_hex).await?;
                    let cid_as_hex = nodecommand::midhex_to_cidhex(mid_as_hex).await?;
                    match node_manager.spread_ntwk(cid_as_hex.to_string()).await
                    {
                         Ok(()) => println!("File spreaded on DHT successfully!"),
                        Err(e) => eprintln!("Error spreaded on DHT: {}", e),
                    }
                    match node_manager.provide_file(path, cid_as_hex.to_string()).await
                    {
                        Ok(()) => println!("File transmitted successfully!"),
                        Err(e) => eprintln!("Error transmitting file: {}", e),
                    }
                }
            }
            "call" => {
                if parts.len() == 2 {
                    let cid_as_hex = parts[1];
                    //let cid_to_hex = nodecommand::midhex_to_cidhex(mid_as_hex).await?;
                    match node_manager.get_file(cid_as_hex.to_string()).await
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
