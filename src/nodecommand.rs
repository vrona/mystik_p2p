use std::error::Error;

use content_hashing::{newkey, encrypt_file, decrypt_file, STORAGE_DIR};
use metadata_mrkl::{Metadata, MonoMetaTree, cid_to_bytes, bytes_to_cid};
use file_sharing::{RunOption, ArgProvOrGet::{Get, Provide}};


// ╔╗╔═╦═╦═╗╔╗
// ║╚╣╬║═╣╬╚╣╚╗
// ╚═╩═╩═╩══╩═╝
// Current Node - Store file
// return private key and nonce to decrypt file if needed + MID to provided to the network
pub async fn store_file(file_name: &str, file_type: &str, secret: &str, file_path: &str) -> Result<([u8; 32], [u8; 19], [u8; 32]), Box<dyn Error>> {

    // create private key and nonce for file encryption/decryption
    let (private_key, nonce) = newkey().await?;

    // get the file's CID
    let cid = encrypt_file(&private_key, &nonce, file_path).await?; //"../ghost-ground/data/mystikp2p.png"

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), secret.to_string());

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = cid_to_bytes(&metadata
        .create_tree(true)
        .await?)
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
    tree.insert(&mid, &cid_as_leaf)?;

    // Verification of MID CID pair creation
    let midcid = tree.verify_merkle_proof(&mid, &cid_as_leaf)?;

    if !midcid {
        return Err("MID-CID pair not stored in record".into());
    }

    return  Ok((private_key, nonce, mid));
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


pub async fn get_decrypt_file(private_key: [u8; 32], nonce: [u8; 19], cid: &str, destination_path: &str)-> Result<(), Box<dyn Error>> { // "../content-hashing/data_dec/mystikp2p.png"
    decrypt_file(&private_key, &nonce, &cid, destination_path).await?;
    Ok(())
}

pub async fn remove_file(file_name: &str, file_type: &str, secret: &str)-> Result<(), Box<dyn Error>> {

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

    // remove MIDCID
    tree.remove(&mid).expect("Critical error: Failed to remove key. Stopping execution.");
  
    // remove metadata tree
    metadata.remove_record().await.expect("Critical error: Failed to remove metadata tree records. Stopping execution.");
    
    Ok(())
}

// ─────────╔╗
// ╔╦╦═╦══╦═╣╚╦═╗
// ║╔╣╩╣║║║╬║╔╣╩╣
// ╚╝╚═╩╩╩╩═╩═╩═╝

// Remote Node - Ask for file
async fn get_ntwk_file() -> Result<(), Box<dyn Error>> {
    
    let opt = RunOption {
        secret_key_seed: None,
        listen_address: None,
        peer: Some("/ip4/127.0.0.1/udp/9000/quic-v1/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X".parse()?),
        argument: Get { 
            name: "kb.mp4".to_string() },
    };
   
    opt.run().await
}

/// TROUVER UN MOYEN DE PASSER L'ADRESSE DU PEER

async fn provide_ntwk_file(cid: &str) -> Result<(), Box<dyn Error>> {

    let opt = RunOption {
        secret_key_seed: None,
        listen_address: Some("/ip4/127.0.0.1/udp/9000/quic-v1".parse()?),
        peer: None,
        argument: Provide {
            path: format!("{}/{}.bin", STORAGE_DIR, cid).into(), // "data/kb.mp4"
            name: format!("{}.bin", cid)}, // kb.mp4
    };
    opt.run().await
}

// Remote Node - Been asked for file
