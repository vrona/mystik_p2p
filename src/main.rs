use file_sharing::{RunOption, ArgProvOrGet};
use std::error::Error;
mod nodecommand;

/*
> Current Node
via Metadata
I store file
I get file

> Remote Node
via Metadata
I ask for file
I've been asked for a file

> To share files we need to provide

struct RunOption {

    pub secret_key_seed: Option<u8>, /// Secret key seed for deterministic peer ID (or `None` for random).
    pub listen_address: Option<Multiaddr>, /// Address on which to listen (or `None` to listen on 0.0.0.0:0).    
    pub peer: Option<Multiaddr>, /// Peer to dial (optional).
    pub argument: ArgProvOrGet, /// Argument "Provide" or "Get" (optional).
}

*/
#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    //     let mut rng = rand::thread_rng();
//     let mission = format!("{}",rng.next_u32());
 Ok(())
}