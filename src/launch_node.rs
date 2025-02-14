// launch_node.rs

use std::error::Error;
use libp2p::futures::future::pending;
use libp2p::{Multiaddr, multiaddr::Protocol};
use tokio::time::{sleep, Duration};
use file_sharing::view_network; // adjust this import to point to your network module

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Launch the network node.
    // Here we use `None` for the secret_key_seed so that the identity is generated randomly.
    let (mut client, mut _event_stream, mut event_loop) = view_network(None).await?;

    // Get the local PeerId from the event loop (assuming you have added a getter as shown above).
    let local_peer_id = event_loop.local_peer_id();

    // Spawn the event loop in a background task.
    tokio::spawn(async move {
        event_loop.run().await;
    });

    // Start listening on a default address.
    // Here we choose a QUIC address with an ephemeral UDP port.
    let listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
    client.start_listening(listen_addr).await?;
    
    // Give the node a moment to start listening and print its full multiaddr.
    sleep(Duration::from_secs(5)).await;
    
    println!("Node launched and connected to the network.");
    println!("Local PeerId: {}", local_peer_id);
    
    // The EventLoop's NewListenAddr event will also print the full multiaddr including the PeerId,
    // for example: "/ip4/127.0.0.1/udp/12345/quic-v1/p2p/<PeerId>"

    // Block forever so that the node keeps running.
    pending::<()>().await;
    Ok(())
}
