use std::error::Error;

use content_hashing::{newkey, encrypt_file, decrypt_file, remove_stored_file, STORAGE_DIR};
use metadata_mrkl::{Metadata, MonoMetaTree, cid_to_bytes, bytes_to_cid, hex_to_bytes, bytes_to_hex};
use file_sharing::{RunOption, ArgProvOrGet::{Get, Provide}, view_network};
use libp2p::{core::Multiaddr, multiaddr::Protocol, PeerId, futures::StreamExt};

use std::time::Duration;

// ╔╗╔═╦═╦═╗╔╗
// ║╚╣╬║═╣╬╚╣╚╗
// ╚═╩═╩═╩══╩═╝
// Current Node - Store file
// return private key and nonce to decrypt file if needed + MID to provided to the network
pub async fn store_file(file_name: &str, file_type: &str, secret: &str, file_path: &str, listen_addr: &str, peer_bootstap: &str) -> Result<([u8; 32], [u8; 19], [u8; 32]), Box<dyn Error>> {

    // create private key and nonce for file encryption/decryption
    let (private_key, nonce) = newkey().await?;

    // get the file's CID
    let cid = encrypt_file(&private_key, &nonce, file_path).await?; //"../ghost-ground/data/mystikp2p.png"
    println!("CID stored for test: {}", cid);

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), secret.to_string());

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = &metadata
        .create_tree(true)
        .await?;

    let mid_as_key  = cid_to_bytes(&mid)
        .await?;
    
    // prooving that metadata exist    
    let leaves: &[&str] = &[file_name, file_type, secret];
    let meta_exist = metadata.prove_leaf_persistent(leaves).await?;

    if ! meta_exist{
        return Err("Given metadata are not stored in record".into());
    }

    // 2nd level of Merkle (Sparse tree)
    // init tree (Monotree) instance with the persistent database (rocksdb) and hasher (blake3)
    let mut tree = MonoMetaTree::new()?;

    // pairing MID as key and CID as leaf in insert 
    let cid_as_leaf = cid_to_bytes(&cid).await?;
    tree.insert(&mid_as_key, &cid_as_leaf)?;

    // Verification of MID CID pair creation
    let midcid = tree.verify_merkle_proof(&mid_as_key, &cid_as_leaf)?;

    if !midcid {
        return Err("MID-CID pair not stored in record".into());
    }

    let hex_key = bytes_to_hex::<32>(private_key).await;
    println!("TEST 🔑 private_key: {:?}", hex_key);
    let hex_nonce = bytes_to_hex::<19>(nonce).await;
    print!("\n");
    println!("TEST 🔢 nonce: {:?}", hex_nonce);

    // provide the file to the network
    provide_ntwk_file(&cid, listen_addr, peer_bootstap).await?;
    
    return  Ok((private_key, nonce, mid_as_key));
}


// Current Node - Retrieve file
pub async fn get_cid_file(file_name: &str, file_type: &str, secret: &str)-> Result<String, Box<dyn Error>> {

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), secret.to_string());

    // prooving that metadata exist    
    let leaves: &[&str] = &[file_name, file_type, secret];
    let meta_exist = metadata.prove_leaf_persistent(leaves).await?;

    if ! meta_exist{
        return Err("Given metadata are not matching records".into());
    }

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = cid_to_bytes(&metadata.create_tree(false).await?).await?;

    // 2nd level of Merkle (Sparse tree)
    // init tree (Monotree) instance with the persistent database (rocksdb) and hasher (blake3)
    let mut tree = MonoMetaTree::new()?;

    // get the MID CID pair
    if let Some(bytes_of_cid) = tree.get(&mid)?{
        let cid = bytes_to_cid(bytes_of_cid).await;
        return Ok(cid)
    };

    Ok("No CID founded".into())
} 

pub async fn get_decrypt_file(hex_key: &str, hex_nonce: &str, cid: &str, destination_path: &str)-> Result<(), Box<dyn Error>> { // "../content-hashing/data_dec/mystikp2p.png"
    let private_key: [u8; 32] = hex_to_bytes::<32>(hex_key).await?;
    let nonce: [u8; 19] = hex_to_bytes::<19>(hex_nonce).await?;
    decrypt_file(&private_key, &nonce, &cid, destination_path).await?;
    Ok(())
}

pub async fn remove_file(file_name: &str, file_type: &str, secret: &str)-> Result<bool, Box<dyn Error>> {

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), secret.to_string());

    // prooving that metadata exist    
    let leaves: &[&str] = &[file_name, file_type, secret];
    let meta_exist = metadata.prove_leaf_persistent(leaves).await?;

    if ! meta_exist{
        return Err("Given metadata are not matching records".into());
    }

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = cid_to_bytes(&metadata.create_tree(false).await?).await?;

    // 2nd level of Merkle (Sparse tree)
    // init tree (Monotree) instance with the persistent database (rocksdb) and hasher (blake3)
    let mut tree = MonoMetaTree::new()?;

    // get the MID CID pair
    if let Some(bytes_of_cid) = tree
        .get(&mid)
        .expect("Failed to retrieved CID of content to delete"){

        let cid = bytes_to_cid(bytes_of_cid).await;
        
        // remove encrypted stored file
        remove_stored_file(&cid).await.expect("Critical error: Failed to remove encrypted stored of removed metadata. Stopping execution.");

        // remove metadata tree
        metadata.remove_record().await.expect("Critical error: Failed to remove metadata tree records. Stopping execution.");

        // remove MIDCID
        tree.remove(&mid).expect("Critical error: Failed to remove key. Stopping execution.");

        return Ok(true)
    };

    return Ok(false)
}

// ─────────╔╗
// ╔╦╦═╦══╦═╣╚╦═╗
// ║╔╣╩╣║║║╬║╔╣╩╣
// ╚╝╚═╩╩╩╩═╩═╩═╝

// Remote Node - Ask for file
// async fn get_from_network_file(cid: &str, address: Multiaddr, peer_id: libp2p::PeerId) -> Result<(), Box<dyn Error>> {
    
//     let peer_address = format!("{}",address.with(Protocol::P2p(peer_id))); // /ip4/127.0.0.1/udp/9000/quic-v1/p2p/
//     let opt = RunOption {
//         secret_key_seed: None,
//         listen_address: None,
//         peer: Some(peer_address.parse()?),
//         argument: Get { 
//             name: format!("{}.bin", cid)}, // kb.mp4
//     };
//     opt.run().await
// }

/// TROUVER UN MOYEN DE PASSER L'ADRESSE DU PEER

async fn provide_ntwk_file(cid: &str, listen_addr: &str, peer_bootstap: &str) -> Result<(), Box<dyn Error>> {

    let opt = RunOption {
        secret_key_seed: None,
        listen_address:Some(listen_addr.parse()?), //Some("/ip4/127.0.0.1/udp/9000/quic-v1".parse()?), //Some(listen_addr.parse()?),
        peer: Some(peer_bootstap.parse()?),
        argument: Provide {
            path: format!("{}/{}.bin", STORAGE_DIR, cid).into(),
            name: format!("{}.bin", cid)},
    };
    opt.run().await
}

// Remote Node - Been asked for file
// pub async fn get_from_network_file(cid: &str, bootstrap_addr: Multiaddr) -> Result<(), Box<dyn Error>> {
//     // Create the network using our public wrapper (which calls the internal network::new).
//     let (mut network_client, mut _network_events, mut network_event_loop) =
//         view_network(None).await?;
    
//     // Spawn the network event loop.
//     tokio::spawn(async move {
//         network_event_loop.run().await;
//     });
    
//     // Dial the bootstrap node so that we join the DHT.
//     if let Some(peer_component) = bootstrap_addr.iter().last() {
//         if let Protocol::P2p(bootstrap_peer_id) = peer_component {
//             network_client.dial(bootstrap_peer_id, bootstrap_addr.clone()).await?;
//         } else {
//             return Err("Bootstrap address must contain a PeerId.".into());
//         }
//     } else {
//         return Err("Invalid bootstrap address.".into());
//     }
    
//     // Give the network a moment for the DHT to update.
//     tokio::time::sleep(Duration::from_secs(2)).await;
    
//     // Form the file name (this must match exactly what the provider advertised).
//     let file_name = format!("{}.bin", cid);
    
//     // Query the DHT for providers of this file.
//     let providers = network_client.get_providers(file_name.clone()).await?;
//     if providers.is_empty() {
//         return Err(format!("No provider found for file: {}", file_name).into());
//     }
    
//     // Pick the first provider.
//     let provider_peer_id = *providers.iter().next().unwrap();
//     println!("Found provider PeerId: {:?}", provider_peer_id);
    
//     // At this point, you only have the PeerId.
//     // You need to get the full network address (Multiaddr) to dial the provider.
//     // Here we assume the provider is listening on a known base address.
//     // In a real-world scenario, you might use the Identify protocol or other mechanisms.
//     let base_addr: Multiaddr = "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?;
//     let provider_full_addr = base_addr.with(Protocol::P2p(provider_peer_id));
//     println!("Dialing provider at: {:?}", provider_full_addr);
    
//     // Now build a RunOption for the GET operation using the discovered address.
//     let opt = RunOption {
//         secret_key_seed: None,
//         listen_address: None,
//         peer: Some(provider_full_addr),
//         argument: Get { name: file_name },
//     };
    
//     // Run the file retrieval.
//     opt.run().await
// }