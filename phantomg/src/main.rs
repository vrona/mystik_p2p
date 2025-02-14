use std::error::Error;
use rand::RngCore;
use dotenv::dotenv;
use std::env;

use nodecommand::{store_file, get_cid_file, get_decrypt_file};

#[tokio::main]
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

    let peer_id_bootsrap = "/ip4/127.0.0.1/udp/56810/quic-v1/p2p/12D3KooWPAJn97YUYasrc5GGWyNNbxLEjnix66LBqQSRYTNsxW8M";
    let (private_key, nonce, _) = store_file(&file_name, &file_type, &secret, &file_path, &peer_id_bootsrap).await?;
    // println!("Private_key: {:?}\n,Nonce: {:?}\n,MID: {:?}", private_key, nonce, mid);

    let returned_cid = get_cid_file(&file_name, &file_type, &secret).await?;
    let _ = get_decrypt_file(private_key, nonce, &returned_cid, &decrypt_path).await;

    Ok(())

    // ORIGINAL TEST FILE-SHARING
    // let opt = RunOption {
    //     secret_key_seed: Some(1),
    //     listen_address: Some("/ip4/127.0.0.1/udp/9000/quic-v1".parse()?),
    //     peer: None,
    //     argument: ArgProvOrGet::Provide {
    //         path: "data/kb.mp4".into(),
    //         name: "kb.mp4".to_string() },
    // };
    // opt.run().await
}