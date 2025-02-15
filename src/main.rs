// launch_node.rs
mod node_config; // Import the configuration module
use std::error::Error;
use dotenv::dotenv;
use std::env;
use std::io::{self, Write, BufRead};
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
        if let Ok(addr_str) = env::var(var) {
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

    // NODE CONFIG
    // Load node configuration
    let config = node_config::load_node_config()?;

    // println!("Enter secret seed (or leave blank for random): ");
    // io::stdout().flush()?; // ensure the prompt is printed

    // let mut seed_input = String::new();
    // io::stdin().read_line(&mut seed_input)?;
    // let secret_seed = if seed_input.trim().is_empty() {
    //     None
    // } else {
    //     // Parse the input as an unsigned 8-bit integer.
    //     Some(seed_input.trim().parse::<u8>()?)
    // };


    // 1. Initialize the network.
    // Here we use `None` for the secret_key_seed (i.e. generate a random identity).
    let (mut client, mut _event_stream, network_event_loop) = view_network(config.secret_seed).await?;
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
    if let Some(addr) = config.listen_addr {
        //listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
        client.start_listening(addr).await?;

    } else {
         println!("Warning: LISTEN_ADDRESS not set, using default.");
    }

    // Wait a moment for listening to be established.
    sleep(Duration::from_secs(5)).await;
    println!("Node launched and connected to the network.");

    // 2.2 Dynamic bootstrap() method on Kad here if desired.)
    // For example, if you have access to the event loop:
    tokio::spawn({
    let network_event_loop = network_event_loop.clone();
        async move {
            let mut event_loop_lock = network_event_loop.lock().await;
            match event_loop_lock.bootstrap() {
                Ok(query_id) => println!("Bootstrap initiated: {:?}", query_id),
                Err(e) => println!("Bootstrap failed: {:?}", e),
            }
        }
    });

    // 2.1 Explicit Bootstrapping:
    let _ = dial_bootstrap_if_needed(&mut client, &local_peer_id).await;
     // Dial bootstrap nodes if needed
    // for bootstrap_addr in &[config.bootstrap_addr_1, config.bootstrap_addr_2] {
    //     if let Some(addr) = bootstrap_addr {
    //         if let Ok(parsed_addr) = addr.parse::<Multiaddr>() {
    //             println!("Dialing bootstrap address: {}", addr);
    //             client.dial(local_peer_id, parsed_addr).await?;
    //         }
    //     }
    // }

    // 3. Unified Interface for Node Commands:
    // Provide a simple command-line loop that allows the user (or an AI agent)
    // to call high-level functions such as store_file() and get_cid_file().
    
    println!(
        "Enter command: (
        store <file_name> <file_type> <secret> <file_path>
        | spread <mid (metadata id)>
        | call <mid (metadata id)>
        | get <file_name> <file_type> <secret> <private_key> <nonce>
        | exit)");
    
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
                let listen_addr = "/ip4/0.0.0.0/udp/0/quic-v1";
                let peer_bootstrap = "/ip4/127.0.0.1/udp/9001/quic-v1/p2p/12D3KooWMzg8FK5YW9SSov78gYudoeJxsWy4GbjMg7fA6JKv2X8T";//env::var("BOOTSTRAP_ADDR_2")?;
                
                match nodecommand::store_file(
                    file_name,
                    file_type,
                    secret,
                    file_path,
                    listen_addr,
                    &peer_bootstrap,
                ).await {
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
                    let listen_addr = "/ip4/0.0.0.0/udp/0/quic-v1";
                    let peer_bootstrap = "/ip4/127.0.0.1/udp/9001/quic-v1/p2p/12D3KooWMzg8FK5YW9SSov78gYudoeJxsWy4GbjMg7fA6JKv2X8T";//env::var("BOOTSTRAP_ADDR_2")?;
                    
                    match nodecommand::spread_file_ntwk(mid_as_hex, listen_addr, peer_bootstrap).await {
                        Ok(()) => print!("File transmitted successfully!"),
                        Err(e) => eprintln!("Error transmitting file: {}", e),
                        
                    }
                }
            }
            "call" => {
                if parts.len() == 2 {
                    let mid_as_hex = parts[1];
                    let peer_bootstrap = "/ip4/127.0.0.1/udp/9001/quic-v1/p2p/12D3KooWMzg8FK5YW9SSov78gYudoeJxsWy4GbjMg7fA6JKv2X8T";//env::var("BOOTSTRAP_ADDR_2")?;
                    
                    match nodecommand::call_file_ntwk(mid_as_hex, peer_bootstrap).await {
                        Ok(()) => print!("File transmitted successfully!"),
                        Err(e) => eprintln!("Error transmitting file: {}", e),
                        
                    }
                }
            }
            "get" => {

                println!("🔍 Raw Input: '{}'", input); // Debugging

                // Extract command arguments
                let parts: Vec<&str> = input.trim().splitn(6, ' ').collect();
                if parts.len() < 6 {
                    eprintln!("Error: 'get' command requires at least 6 arguments but got {}", parts.len());
                    continue;
                }

                let file_name = parts[1];
                let file_type = parts[2];
                let secret = parts[3];

                // parts[4] should now contain the full private_key string, e.g. "120, 160, 253, ... , 250"
                let private_key = parts[4];
                let nonce = parts[5];

                match nodecommand::get_cid_file(
                    file_name,
                    file_type,
                    secret
                ).await {
                    Ok(returned_cid) => {
                        // let env_decrypt_path = env::var("decrypt_path").expect("decrypt_path not found in .env");
                        println!("Debug: Returned CID: {:?}", returned_cid);
                        let decrypt_path = format!("/Volumes/vrona_SSD/rust_premiere/backup_old_mystik>p2p/data/dec/{}{}", file_name, file_type);
                        nodecommand::get_decrypt_file(private_key, nonce, &returned_cid, &decrypt_path).await?;
                        println!("File retrieved successfully.")
                    },
                    Err(e) => eprintln!("Error retrieving file: {}", e),
                }
            }
            "exit" => break,
            _ => println!("Unknown command. Usage: store <file_name> <file_type> <secret> <file_path> | get <file_name> <file_type> <secret> <private_key> <nonce> | exit"),
        }
        println!("Enter command: (store <file_name> <file_type> <secret> <file_path> | get <file_name> <file_type> <secret> <private_key> <nonce> | exit)");
    }

    // Keep the node running.
    pending::<()>().await;
    Ok(())
}


 