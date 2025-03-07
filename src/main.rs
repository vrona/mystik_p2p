// main.rs

mod node_config; // Our configuration module

use std::{error::Error, sync::Arc};
use std::io::{self, BufRead, Write};
use tokio::{sync::Mutex, time::{sleep, Duration}};

use libp2p::Multiaddr;
use tracing_subscriber::EnvFilter;
use futures::{future::pending, Stream};
// Import the public interface from each node crate.
use node_autonomos::AutonomosManager; // This crate implements the regular (autonomos) node.
use node_syndesmos::SyndesmosManager; // This crate implements the rendezvous (syndesmos) node.

// Optionally, also import your node command functions.
use nodecommand;

use content_hashing::get_storage_dir;
// prompt the command
fn print_prompt_command() -> io::Result<()>{
    println!(
            "Enter command: (store <file_name> <file_type> <secret> <file_path>\
            | spread <mid (metadata id)>\
            | call <file_name> <file_type> <secret>\
            | get <file_name> <file_type> <secret> <private_key> <nonce>\
            | exit)"
        );
    io::stdout().flush()
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Ask the user which node type to launch.
    println!("Select node type: (1 = Syndesmos, 2 = Autonomos)");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let choice = input.trim();

    // Branch according to user selection.
    if choice == "1" {
        // Load configuration from our node_config module.
        let config = node_config::load_node_config("1")?;
        
        let listen_address: Option<Multiaddr> = config.listen_addr;
        // For simplicity we use None for bootstrap/peer here.
        let peer: Option<Multiaddr> = None;
        
        println!("Launching Syndesmos (Rendezvous Node)...");

        let syndesmos_node =
            SyndesmosManager::new(config.secret_seed, listen_address.clone(), peer).await?;

        // Wait a moment for listening to be established.
        sleep(Duration::from_secs(5)).await;
        println!("Node launched and connected to the network.");
        
        let event_loop_arc = syndesmos_node.event_loop.clone();

        // Spawn the event loop.
        {
            let event_loop_clone = event_loop_arc.clone();
            tokio::spawn(async move {
                event_loop_clone.lock().await.run().await;
            });
        }

        println!("Syndesmos node launched and connected to the network.");
        
        // Command-line interface loop for the syndesmos node.
        command_loop_syndesmos(syndesmos_node).await?;

        // Keep the node running.
        pending::<()>().await;

        } else if choice == "2" {

            // Load configuration from our node_config module.
            let config = node_config::load_node_config("2")?;

            let listen_address: Option<Multiaddr> = None;
            // For simplicity we use None for bootstrap/peer here.
            let peer: Option<Multiaddr> = None;

            println!("Launching Autonomos (Regular Node)...");

            // let mut autonomos_node = node_autonomos::AutonomosManager::new(config.secret_seed, listen_address.clone(), peer).await?;
            let autonomos_node =
                Arc::new(Mutex::new(node_autonomos::AutonomosManager::new(config.secret_seed, listen_address.clone(), peer).await?));
            
            // Wait a moment for listening to be established.
            sleep(Duration::from_secs(5)).await;
            println!("Node launched and connected to the network.");

            // let event_loop_arc = autonomos_node.event_loop.clone();

            // Spawn the event loop.
            {
                //let event_loop_clone = event_loop_arc.clone();
                let event_loop_arc = autonomos_node.lock().await.event_loop.clone();
                tokio::spawn(async move {
                    event_loop_arc.lock().await.run().await;
                    // event_loop_clone.lock().await.run().await;
                });
            }
            // Extract the event loop clone from the autonomos node.
            // let event_loop_arc = {
            //     let node = autonomos_node.lock().await;
            //     node.event_loop.clone()
            // };

            // // Create a LocalSet to run non-Send futures.
            // let local = tokio::task::LocalSet::new();

            // // Spawn the event loop as a local task.
            // local.spawn_local({
            // let event_loop_arc = event_loop_arc.clone();
            // async move {
            //         event_loop_arc.lock().await.run().await;
            //     }
            // });


            let namespace = "rendezvous";
            let _register = autonomos_node.lock().await.register_rendezvous(
                config.fix_addr_1.unwrap(), // "/ip4/127.0.0.1/udp/9000/quic-v1".parse::<Multiaddr>().unwrap(), // , 
                config.peer_id_1.unwrap(), 
                namespace).await;
            

            println!("Autonomos node launched and connected to the network.");
            
            // Command-line interface loop for the autonomos node.
            command_loop_autonomos(autonomos_node).await?;
        } else {
            println!("Invalid selection. Exiting.");
        }

        Ok(())
}

/// Command loop for Syndesmos (rendezvous node)
async fn command_loop_syndesmos(

    mut syndesmos: SyndesmosManager

) -> Result<(), Box<dyn Error>> {
    
    // Command-line interface.
    let _print_command = print_prompt_command()?;

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
                    Ok((private_key, nonce, _mid_as_hex, _cid_as_hex)) => {
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
                    match syndesmos.spread_ntwk(cid_as_hex.to_string()).await
                    {
                        Ok(()) => println!("File spreaded on DHT successfully!"),
                        Err(e) => eprintln!("Error spreaded on DHT: {}", e),
                    }
                    match syndesmos.provide_file(path, cid_as_hex.to_string(), cid_as_hex.to_string()).await
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
                    match syndesmos.get_file(cid_as_hex.to_string()).await
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
                        println!("get - Debug: Returned CID: {:?}", returned_cid);
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
                           | call <file_name> <file_type> <secret>\
                           | get <file_name> <file_type> <secret> <private_key> <nonce>\
                           | exit"),
        }
        let _print_command = print_prompt_command()?;
    }
    Ok(())
}

/// Command loop for Autonomos (regular node)
async fn command_loop_autonomos(

    autonomos: Arc<Mutex<AutonomosManager>>

) -> Result<(), Box<dyn Error>> {
    
    // Command-line interface.
    let _print_command = print_prompt_command()?;

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

                let tmp_constant_path = std::path::PathBuf::from(get_storage_dir());
                match nodecommand::store_file(
                    file_name,
                    file_type,
                    secret,
                    file_path
                )
                .await
                {
                    Ok((private_key, nonce, mid_as_hex, cid_as_hex)) => {
                        println!("🔍 private_key: {:?}", private_key);
                        println!("🔍 nonce: {:?}", nonce);

                        //Clone the Arc to call spread_ntwk
                        {
                            let autonomos_clone = Arc::clone(&autonomos);
                            let mid = mid_as_hex.clone();
                            let cid = cid_as_hex.clone();
                            let result = {
                                let mut auto = autonomos_clone.lock().await;
                                auto.register_midcid_memory(mid.to_string(), tmp_constant_path, cid).await
                            };
                            match result {
                            Ok(()) => println!("Registered new midcid pair successfully."),
                            Err(e) => eprintln!("Error Registering midcid pair: {}", e),
                            }
                        }
                            
                    }
                    Err(e) => eprintln!("Error storing file: {}", e),
                }
            }
            "spread" => {
                if parts.len() == 2 {
                    let mid_as_hex = parts[1].to_string();
                    // let path = nodecommand::local_path_to_cid(&mid_as_hex).await?;
                    // let cid_as_hex = nodecommand::midhex_to_cidhex(&mid_as_hex).await?;
                    
                    //Clone the Arc to call spread_ntwk
                    {
                        let autonomos_clone = Arc::clone(&autonomos);
                        let mid = mid_as_hex.clone();
                        let result = {
                            let mut auto = autonomos_clone.lock().await;
                            auto.spread_ntwk(mid.to_string()).await
                        };
                        match result {
                        Ok(()) => println!("File spreaded on DHT successfully!"),
                        Err(e) => eprintln!("Error spreaded on DHT: {}", e),
                        }
                    }
                    // Spawn the inbound answer loop as a separate task.
                    let autonomos_clone = autonomos.clone(); // <-- clone the Arc!
                    tokio::spawn(async move {
                        let mut auto = autonomos_clone.lock().await;
                        if let Err(e) = auto.launch_inbound_request_task().await {
                            eprintln!("Error transmitting file in inbound handler: {}", e);
                        }
                    });


                    // Spawn the inbound request task as a local task.
                    // {
                    //     let autonomos_clone = Arc::clone(&autonomos_node);
                    //     local.spawn_local(async move {
                    //         if let Err(e) = autonomos_clone.lock().await.launch_inbound_request_task().await {
                    //             eprintln!("Error in inbound request task: {}", e);
                    //         }
                    //     });
                    // }
                    
                }
            }
            "call" => {
                if parts.len() == 4 {
                    let file_name = parts[1];
                    let file_type = parts[2];
                    let secret = parts[3];

                    match nodecommand::propose_mid_ntwk(file_name, file_type, secret).await {
                        
                        Ok(returned_mid_hex) => {
                            println!("call - Debug: Returned MID: {:?}", returned_mid_hex);
                            {
                                let autonomos_clone = Arc::clone(&autonomos);
                                let returned_mid = returned_mid_hex.clone();
                                let result = {
                                    let mut auto = autonomos_clone.lock().await;
                                    auto.get_file(returned_mid).await
                                };
                                match result {
                                    Ok(cid_as_hex) => {
                                        println!("File transmitted successfully!");
                                        match nodecommand::import_ntwk_file(&returned_mid_hex, &cid_as_hex).await {
                                            Ok(()) => println!("Import succeeded!"),
                                            Err(e) => eprintln!("Error importing file: {}", e),
                                        }
                                    }
                                    Err(e) => eprintln!("Error transmitting file: {}", e),
                                }
                            }
                        }
                        Err(e) => eprintln!("Error retrieving file: {}", e),
                    }
                }
            }
            //                 match autonomos.get_file(returned_mid_hex.clone()).await
            //                 {
            //                     Ok(cid_as_hex) => {
            //                         println!("File transmitted successfully!");
                                
            //                     match  nodecommand::import_ntwk_file(&returned_mid_hex, &cid_as_hex).await
            //                     {
            //                         Ok(()) => println!("!"),
            //                         Err(e) => eprintln!("Error importing file: {}", e),
            //                     }
            //                 }
            //                     Err(e) => eprintln!("Error transmitting file: {}", e),
            //                 }

                            
            //             }

            //             Err(e) => eprintln!("Error retrieving file: {}", e),
            //         }
            //     }
            // }
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
                        println!("get - Debug: Returned CID: {:?}", returned_cid);
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
                           | call <file_name> <file_type> <secret>\
                           | get <file_name> <file_type> <secret> <private_key> <nonce>\
                           | exit"),
        }
        let _print_command = print_prompt_command()?;
    }
    // Keep the node running indefinitely.
    pending::<()>().await;
    Ok(())
}