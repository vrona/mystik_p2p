use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::path::Path;


use anyhow::{anyhow, Error, Ok, Result};


use blake3::Hasher;

use rand::{thread_rng, RngCore};

const STORAGE_DIR: &str = "data/";

///// STORING SECTION
/// save the file to the storage folder and return its CID
async fn store_file(file_path: &str, metadata: &str) -> Result<String, Error> {

    // open the file for reading
    let mut file = fs::File::open(file_path)?;
    
    let mut hasher = Hasher::new();

    // read the file in chunks and hash it
    let mut buffer = vec![0u8; 64 * 1024]; // 64 KiB chunks

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    // finalize the hash and get the CID
    let cid = hasher.finalize().to_hex().to_string();

    // move the file to the storage folder based on CID
    let storage_path = format!("{}/{}/{}", STORAGE_DIR, metadata, cid);

    // create the storage folder if it does not exist
    if let Some(parent) = Path::new(&storage_path).parent(){
        fs::create_dir_all(parent)?;
    } else {
        return Err(anyhow!("Failed to created path because of non existing parent"))
    }

    // move the file to the storage folder
    fs::copy(file_path, &storage_path)?;

    Ok(cid)
}

async fn retrieve_file(cid: &str) -> Result<Vec<u8>, Error> {
    
    // construct the file path based on the CID
    let storage_path = format!("{}/{}/{}", STORAGE_DIR, &cid[0..2], cid);
    
    // read the file content
    let file_content = fs::read(storage_path)?;

    // verify the file content hash
    let mut hasher = Hasher::new();
    hasher.update(&file_content);
    let file_hash = hasher.finalize().to_hex().to_string();

    if file_hash != cid {
        return Err(anyhow!("File content hash does not match CID"));
    }
    
    Ok(file_content)
}

#[tokio::main]
async fn main() -> Result<(), Error>{

    let file_cid = store_file("../ghost-ground/data/kb.mp4", "solar").await?;
    println!("CID: {}", file_cid);

    let file_from_cid = retrieve_file(&file_cid).await?;

    println!("Retrieved file content: {} bytes", file_from_cid.len());

    Ok(())

}

// async fn check_file_exists(path: &str) -> Result<bool> {
//     Ok(fs::metadata(path).is_ok())
// }

// async fn read_file(path: &str) -> Result<Vec<u8>> {
//     Ok(fs::read(path)?)
// }

// async fn write_file(path: &str, data: Vec<u8>) -> Result<()> { Ok(()) }


// async fn create_folder_storage() -> Result<()> {
//     let path = Path::new("data");

//     if path.exists() {
//         // data storage folder does exist
//         return Ok(());
//     } else {
//         // data storage folder does not exist
//         fs::create_dir_all(path)?;
//     }
//     Ok(())
// }