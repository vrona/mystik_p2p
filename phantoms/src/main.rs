use file_sharing::{RunOption, ArgProvOrGet};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let opt = RunOption {
        secret_key_seed: None,
        listen_address: None,
        peer: Some("/ip4/127.0.0.1/udp/9000/quic-v1/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X".parse()?),
        argument: ArgProvOrGet::Get { 
            name: "kb.mp4".to_string() },
    };
   
    opt.run().await

}