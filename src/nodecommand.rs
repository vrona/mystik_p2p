use std::error::Error;
use content_hashing::{newkey, encrypt_file, decrypt_file};
use metadata_mrkl::{Metadata, MonoMetaTree, cid_to_bytes, bytes_to_cid};
use file_sharing::*;


// Current Node - Store file
// return private key and nonce to decrypt file if needed + MID to provided to the network
pub async fn store_file(file_name: &str, file_type: &str, mission: &str, file_path: &str) -> Result<([u8; 32], [u8; 19], [u8; 32]), Box<dyn Error>> {

    // create private key and nonce for file encryption/decryption
    let (private_key, nonce) = newkey().await?;

    // get the file's CID
    let cid = encrypt_file(&private_key, &nonce, file_path).await?; //"../ghost-ground/data/mystikp2p.png"

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), mission.to_string());

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = cid_to_bytes(&metadata
        .create_tree(true)
        .await?)
        .await?;
    
    // prooving that metadata exist    
    let leaves: &[&str] = &[file_name, file_type, mission];
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
pub async fn get_cid_file(file_name: &str, file_type: &str, mission: &str)-> Result<String, Box<dyn Error>> {

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), mission.to_string());

    // prooving that metadata exist    
    let leaves: &[&str] = &[file_name, file_type, mission];
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
//     let cid = get
//     
//     Ok(())
// } 
//     // reverse the peedID 
//     let cid_recomposed = bytes_to_cid(cid_as_leaf).await;
 
 
// Remote Node - Ask for file

// Remote Node - Been asked for file
