use rand::RngCore;
use dotenv::dotenv;
use std::env;
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
    
    dotenv().ok();
    let rng = rand::thread_rng().next_u32();
    let secret = format!("{}",rng);
    let file_name = "mystikp2p".to_string();
    let file_type = ".png".to_string();

    let env_file_path = env::var("file_path").expect("file_path not found in .env");
    let file_path = format!("{}data/{}{}", env_file_path, file_name, file_type);

    let env_decrypt_path = env::var("decrypt_path").expect("decrypt_path not found in .env");
    let decrypt_path = format!("{}data/dec/{}{}", env_decrypt_path, file_name, file_type);

    let (private_key, nonce, _) = nodecommand::store_file(&file_name, &file_type, &secret, &file_path).await?;
    // println!("Private_key: {:?}\n,Nonce: {:?}\n,MID: {:?}", private_key, nonce, mid);

    let returned_cid = nodecommand::get_cid_file(&file_name, &file_type, &secret).await?;
    let _ = nodecommand::get_decrypt_file(private_key, nonce, &returned_cid, &decrypt_path).await?;

    // let is_removed = nodecommand::remove_file(&&file_name, &file_type, &secret).await?;
    // println!("All content is_removed? {}", is_removed);

 Ok(())
}