use dotenv::dotenv;
use std::env;
use std::error::Error;
use std::io::{self, Write};
use libp2p::Multiaddr;

/// Struct to hold node configuration
pub struct NodeConfig {
    pub secret_seed: Option<u8>,
    pub listen_addr: Option<Multiaddr>,
    pub bootstrap_addr_1: Option<String>,
    pub bootstrap_addr_2: Option<String>,
}

/// Function to load the appropriate environment variables
pub fn load_node_config() -> Result<NodeConfig, Box<dyn Error>> {
    // Ask the user which type of node they want to launch
    println!("Select node type: (1 = Bootstrap 1, 2 = Bootstrap 2, Enter = Regular Node)");
    io::stdout().flush()?; // Ensure prompt is printed

    let mut node_input = String::new();
    io::stdin().read_line(&mut node_input)?;
    let node_choice = node_input.trim();

    // Load the correct .env file if this is a bootstrap node
    match node_choice {
        "1" => {
            dotenv::from_filename(".env.bootstrap1").ok();
            println!("Loaded environment file: .env.bootstrap1");
        }
        "2" => {
            dotenv::from_filename(".env.bootstrap2").ok();
            println!("Loaded environment file: .env.bootstrap2");
        }
        "" => {
            println!("Running as a regular node (no bootstrap env loaded).");
            dotenv().ok(); // Load a default .env file if needed
        }
        _ => {
            eprintln!("Invalid choice. Enter 1, 2, or press Enter for a regular node.");
            return Err("Invalid node type selection".into());
        }
    }

    // Read environment variables
    let secret_seed = env::var("SECRET_SEED").ok().and_then(|s| s.parse::<u8>().ok());
    let listen_addr = env::var("LISTEN_ADDRESS").ok().and_then(|s| s.parse::<Multiaddr>().ok());
    let bootstrap_addr_1 = env::var("BOOTSTRAP_ADDR_1").ok();
    let bootstrap_addr_2 = env::var("BOOTSTRAP_ADDR_2").ok();

    Ok(NodeConfig {
        secret_seed,
        listen_addr,
        bootstrap_addr_1,
        bootstrap_addr_2,
    })
}
