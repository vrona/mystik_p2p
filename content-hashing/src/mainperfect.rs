use std::fs::{self, File};
use std::io::{Read, Write, BufReader, BufWriter};
use std::path::Path;
use blake3::Hasher;
use std::result::Result::Ok;

use anyhow::{anyhow, Error, Result};

use chacha20poly1305::aead::{OsRng, stream::{EncryptorBE32, DecryptorBE32}};

use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;

const STORAGE_DIR: &str = "../content-hashing/data/";
const BUFFER_SIZE: usize = 1024 * 2048;

///// CRYPTOGRAPHY SECTION
async fn newkey() -> Result<([u8; 32], [u8; 19]), Error> {

    let mut key = [0u8; 32];
    let mut nonce = [0u8; 19];

    OsRng.fill_bytes(&mut key);
    OsRng.fill_bytes(&mut nonce);
    
    Ok((key, nonce))
}

async fn check_cid(encryption_case: bool, cid: Option<&str>, input_doc_path: Option<&str>) -> Result<bool, Error>{
    /*
    2 different behaviors:
    - encryption case: chech_cid(true, cid needed), encryption is aborded if:
        > data folder or CID not provided,
        > CID founded in data folder. 
    
    - Decryption case: chech_cid(false, input_doc_path needed), decryption is aborded if:
        > input_doc_path not provided,
        > CID not founded in data folder. 
    */

    // while in encryption case
    if encryption_case {

        if let Some(value) = input_doc_path {

            
            //let searching_cid = format!("{}.tmp", value);

            // Iterate over the encrypted data directory
            if !fs::exists(value)? {
                //println!("Mission does not exist yet");
                return Ok(false);

            } else {

                match fs::read_dir(value) {
                
                // Storage DIR exist
                    Ok(entries) => {                        
                        for entry in entries {
                            if let Ok(entry) = entry {
                                let path = entry.path();

                                if let Some(name) = path.file_name() {

                                    if let Some(name_str) = name.to_str(){
                                        if let Some(value_cid) = cid {
                                            let file_cid = format!("{}.bin", value_cid);
                                   
                                            if name_str == file_cid {
                                                //println!("Encryption case: CID: {} been found", value);
                                                return Ok(true);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        //println!("Encryption case: CID existence is {}", value);
                        return Ok(false);

                    }
                    // Storage DIR does NOT exist
                    Err(e) => {
                        println!("Encryption case: Error reading directory: {}", e);
                        return Ok(false);
                    }
                }            
            }
        } else {
            println!("Encryption case: No CID provided");
            return Ok(false);
        }
    
    } else {
        // while in decryption case
        
        if let Some(value) = input_doc_path {

            if fs::exists(value)? {
    
                let mut file_encrypted = File::open(value)
                    .map_err(|e|anyhow!("Failed to open file_encrypted {}", e))?;
            
                let mut buffer_check = vec![0u8; BUFFER_SIZE + 16];

                let mut hasher = Hasher::new();

                loop {
                    let n = file_encrypted.read(&mut buffer_check)?;
                    if n == 0 {
                        break
                    }
                    hasher.update(&buffer_check[..n]);   
                }

                let tmp_cid = hasher.finalize().to_hex().to_string();

                if tmp_cid != cid.unwrap() {
                    print!("Decryption case: File content hash does not match CID");
                    return  Ok(false);

                } else {
                    //print!("Decryption case: File content hash match CID");
                    return Ok(true);
                }

            } else {
                print!("Document does not exist at this path.");
                return Ok(false);
            }

        } else {
            println!("Decryption case: No input doc path provided");
            return Ok(false);
        }
    }

}

/// Encrypt file
async fn encrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    input_path: &str,
    metadata: &str) -> Result<String, Error> {
    
    let mut hasher = Hasher::new();

    let tmp_output_path = format!("{}encdoc.tmp", STORAGE_DIR);
    
    let mut tmp_output = BufWriter::new(File::create(&tmp_output_path)?);

    let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into());

    let mut input = BufReader::new(File::open(input_path)?);

    let mut buffer = vec![0u8; BUFFER_SIZE];

    loop {
        let read_bytes = input.read(&mut buffer)?;
        
        if read_bytes == BUFFER_SIZE {

            let ciphertext = stream_encryptor
                .encrypt_next(buffer.as_slice())
                .map_err(|e|anyhow!("Failed to encrypt ciphertext chunk {}", e))?;
            tmp_output.write(&ciphertext)?;
            hasher.update(&ciphertext);

        } else {

            let ciphertext = stream_encryptor
                .encrypt_last(&buffer[..read_bytes])
                .map_err(|e| anyhow!("Failed to encrypt last chunk {}", e))?;
            tmp_output.write(&ciphertext)?;
            hasher.update(&ciphertext);
            break;
        }
    
        tmp_output.flush()?;
    }

    // finalize the hash and get the CID
    let cid = hasher.finalize().to_hex().to_string();

    // CHECK existence of CID in data storage
    let cid_path = format!("{}{}/", STORAGE_DIR, metadata);

    let is_cid_exist = check_cid(true, Some(&cid), Some(&cid_path)).await?;

    if !is_cid_exist {
        
        let final_path = format!("{}{}.bin", cid_path, cid);

        // Ensure the parent directory is created.
        if let Some(parent) = Path::new(&final_path).parent(){
            fs::create_dir_all(parent)?;
        } else {
            return Err(anyhow!("Failed to created path because of non existing parent"))
        }
        // Move/rename the .tmp file to the final path
        fs::rename(&tmp_output_path, &final_path)?;

        println!("Encryption done!");
        Ok(cid)

    } else {
        fs::remove_file(&tmp_output_path)?;
        return Err(anyhow!("Document already exist, upload and encryption aborded."))
    }
}


async fn decrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    cid: &str,
    metadata: &str,
    output_path: &str
) -> Result<(), Error> {
   
    let input_doc_path = format!("{}{}/{}.bin", STORAGE_DIR, metadata, cid);

    // CHECK existence of CID in data storage
    let is_cid_exist = check_cid(false, Some(&cid), Some(&input_doc_path)).await?;

    if is_cid_exist {

        let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
        let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());

        let mut input = BufReader::new(File::open(&input_doc_path)?);
        
        let mut output = BufWriter::new(File::create(output_path)?);

        let mut buffer = vec![0u8; BUFFER_SIZE + 16]; // +16 for tag overhead

        loop {
            let read_bytes = input.read(&mut buffer)?;

            if read_bytes == BUFFER_SIZE + 16 {

                let cleartext = stream_decryptor
                    .decrypt_next(buffer.as_slice())
                    .map_err(|e|anyhow!("Failed to decrypt cleartext chunk {}", e))?;
                output.write(&cleartext)?;

            } else if read_bytes == 0 {
                break

            } else {

                let cleartext = stream_decryptor
                    .decrypt_last(&buffer[..read_bytes])
                    .map_err(|e| anyhow!("Failed to decrypt last chunk {}", e))?;
                output.write(&cleartext)?;
                break;
            }
            output.flush()?;
        }

    println!("Decryption done!");
    }

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Error>{

    let (new_key, new_nonce) = newkey().await?;
    let mut rng = rand::thread_rng();
    
    //let (new_key2, new_nonce2) = newkey().await?;
    let mission = format!("{}",rng.next_u32());

    let cid = encrypt_file(&new_key, &new_nonce, "../ghost-ground/data/mystikp2p.png", &mission).await?;
    
    decrypt_file(&new_key, &new_nonce, &cid, &mission, "../content-hashing/data_dec/mystikp2p.png").await?;
    

    encrypt_file(&new_key, &new_nonce, "../ghost-ground/data/mystikp2p.png", &mission).await?;

    

    Ok(())
}
