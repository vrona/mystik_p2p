use file_sharing::{RunOption, ArgProvOrGet, view_network};
use std::error::Error;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let opt = RunOption {
        secret_key_seed: None,
        listen_address:  None,
        peer: Some("/ip4/192.168.1.42/udp/62404/quic-v1/p2p/12D3KooWSEphxvRh3cwBTHmDQQPw2EJ6aF9ChR4SwaCnUoC8NdTR".parse()?),
        argument: ArgProvOrGet::Get { 
            name: "fc37582014a9a3964e2492fbad329f1aae8a9b0acc55e831f3452b465f89d55d.bin".to_string() },
    };
   
    opt.run().await?;

    Ok(())

}

    //Some("/ip4/127.0.0.1/udp/9000/quic-v1/p2p/12D3KooWP86ZTdz3bNgV2icc8FKFftHFTsEc9fiCJTu8tXrpK2nK".parse()?),

    // let (mut network_client, mut _network_events, mut _network_event_loop) = view_network(None).await?;
    // let filename = "d0da60bd015a192100b2b6a9cdeb9563ea5b0c09070fd8ffd9f26922f442c707.bin".to_string();
    // let provider = network_client.get_providers(filename).await?;
    // let provider_peer_id = *provider.iter().next().unwrap();
    // println!("Found provider PeerId: {:?}", provider_peer_id);