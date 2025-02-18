use futures::prelude::*;
use libp2p::{
    identity,
    kad,
    relay,
    ping,
    identify,
    Multiaddr,
    PeerId,
    SwarmBuilder,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    StreamProtocol,
};
use libp2p_stream as stream; // assuming your file exchange protocol uses libp2p_stream
use std::error::Error;
use std::time::Duration;
use tokio::runtime::Runtime;
use tracing_subscriber;

// Define a custom protocol tag for file exchange.
const FILE_PROTOCOL: StreamProtocol = StreamProtocol::new("/file-exchange/1");

/// CompositeBehaviour merges relay, ping, identify, Kademlia, and stream behaviours.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "CompositeEvent")]
pub struct CompositeBehaviour {
    pub relay: relay::Behaviour,
    pub ping: ping::Behaviour,
    pub identify: identify::Behaviour,
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,
    pub stream: stream::Behaviour,
}

/// CompositeEvent wraps the events from the individual protocols.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum CompositeEvent {
    Relay(relay::Event),
    Ping(ping::Event),
    Identify(identify::Event),
    Kademlia(kad::Event),
    // libp2p_stream::Behaviour does not emit events by default.
    Stream(()),
}

impl From<relay::Event> for CompositeEvent {
    fn from(event: relay::Event) -> Self {
        CompositeEvent::Relay(event)
    }
}

impl From<ping::Event> for CompositeEvent {
    fn from(event: ping::Event) -> Self {
        CompositeEvent::Ping(event)
    }
}

impl From<identify::Event> for CompositeEvent {
    fn from(event: identify::Event) -> Self {
        CompositeEvent::Identify(event)
    }
}

impl From<kad::Event> for CompositeEvent {
    fn from(event: kad::Event) -> Self {
        CompositeEvent::Kademlia(event)
    }
}

impl From<()> for CompositeEvent {
    fn from(_: ()) -> Self {
        CompositeEvent::Stream(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging.
    tracing_subscriber::fmt::init();

    // Generate a key pair and determine the local peer id.
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = local_key.public().to_peer_id();
    println!("Local peer id: {:?}", local_peer_id);

    // Build the composite swarm.
    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_quic() // Using QUIC transport; adjust as needed.
        .with_behaviour(|key| {
            // Build each sub-behaviour:
            let relay_behaviour = relay::Behaviour::new(key.public().to_peer_id(), Default::default());
            let ping_behaviour = ping::Behaviour::new(
                ping::Config::new().with_interval(Duration::from_secs(10))
            );
            let identify_behaviour = identify::Behaviour::new(
                identify::Config::new("/composite/0.1.0".to_string(), key.public())
            );
            // Build Kademlia with an in-memory store. We set it to server mode.
            let store = kad::store::MemoryStore::new(key.public().to_peer_id());
            let mut kademlia_behaviour = kad::Behaviour::new(
                local_peer_id, store);
            kademlia_behaviour.set_mode(Some(kad::Mode::Server));
            // Build the stream behaviour (e.g. for file exchange).
            let stream_behaviour = stream::Behaviour::new();

            CompositeBehaviour {
                relay: relay_behaviour,
                ping: ping_behaviour,
                identify: identify_behaviour,
                kademlia: kademlia_behaviour,
                stream: stream_behaviour,
            }
        })?
        .with_swarm_config(|config| {
            // Set an idle connection timeout, etc.
            config.with_idle_connection_timeout(Duration::from_secs(60))
        })
        .build();

    // Listen on a QUIC address (adjust if you want TCP or dual-stack).
    let listen_addr: Multiaddr = "/ip4/0.0.0.0/udp/0/quic-v1".parse()?;
    swarm.listen_on(listen_addr)?;

    // Create a Tokio runtime to drive the swarm.
    let mut rt = Runtime::new()?;
    rt.block_on(async {
        loop {
            match swarm.select_next_some().await {
                SwarmEvent::Behaviour(event) => {
                    println!("Behaviour event: {:?}", event);
                },
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening on {:?}", address);
                },
                other => {
                    // Handle other events as needed.
                    // e.g., dial events, connection events, etc.
                }
            }
        }
    });

    Ok(())
}
