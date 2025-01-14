use file_sharing::{RunOption, ArgProvOrGet};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let opt = RunOption {
        secret_key_seed: Some(1),
        listen_address: Some("/ip4/127.0.0.1/udp/9000/quic-v1".parse()?),
        peer: None,
        argument: ArgProvOrGet::Provide {
            path: "/Volumes/vrona_SSD/intnet/content_test/kb.mp4".into(),
            name: "kb.mp4".to_string() },
    };
   
    opt.run().await

}