use std::error::Error;
use content_hashing::*;
use metadata_mrkl::*;
use file_sharing::*;


// Current Node - Store file
pub async fn store_file(file_name: &str, file_type: &str, mission: &str, file_path: &str) -> Result<(), Box<dyn Error>> {

    // create private key and nonce for file encryption/decryption
    let (new_key, new_nonce) = newkey().await?;

    // get the file's CID
    let cid = encrypt_file(&new_key, &new_nonce, file_path, &mission).await?; //"../ghost-ground/data/mystikp2p.png"

    // init the metadata
    let mut metadata = Metadata::new(file_name.to_string(), file_type.to_string(), mission.to_string(), cid.to_string());

    // 1st level of Merkle (classic tree) get the metadata's MID
    let mid = cid_to_bytes(&metadata.create_tree().await?).await?;

    // 2nd level of Merkle (Sparse tree)
    // init tree (Monotree) instance with the persistent database (rocksdb) and hasher (blake3)
    let mut tree = MonoMetaTree::new()?;

    // pairing MID as key and CID as leaf in insert 
    let cid_as_leaf = cid_to_bytes(&metadata.cid).await?;
    tree.insert(&mid, &cid_as_leaf)?;

    Ok(())
}
    
//     decrypt_file(&new_key, &new_nonce, &cid, &mission, "../content-hashing/data_dec/mystikp2p.png").await?;
//     // decrypt_file(&new_key, &new_nonce, "d83145dc624fe9f277b8bdafbe0fcf5f1ebaa1f216040112ed84e14b38e8dfa2", &mission, "../content-hashing/data_dec/mystikp2p.png").await?; 


// Current Node - Retrieve file
pub async fn get_file(file_name: &str, file_type: &str, mission: &str, destination_path: &str)-> Result<(), Box<dyn Error>> {

    //
    Ok(())
} 
//     // reverse the peedID
//     let cid_recomposed = bytes_to_cid(cid_as_leaf).await;
 
 
// Remote Node - Ask for file

// Remote Node - Been asked for file
