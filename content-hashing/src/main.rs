use core::hash;
use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::path::Path;
use blake3::{hash, Hasher};

use anyhow::{anyhow, Error, Ok, Result};

use chacha20poly1305::aead::{OsRng, stream::{EncryptorBE32, DecryptorBE32}};

use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;

const BUFFER_SIZE: usize = 64 * 1024;
const STORAGE_DIR: &str = "../content-hashing/data/";

///// CRYPTOGRAPHY SECTION
async fn newkey() -> Result<([u8; 32], [u8; 19]), Error> {

    let mut key = [0u8; 32];
    let mut nonce = [0u8; 19];

    OsRng.fill_bytes(&mut key);
    OsRng.fill_bytes(&mut nonce);
    
    Ok((key, nonce))
}

/// Encrypt file
async fn encrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    input_path: &str,
    metadata: &str) -> Result<String, Error> {
    
    let mut hasher = Hasher::new();

    let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into());

    let mut input = BufReader::new(File::open(input_path)?);

    let tmp_output_path = format!("{}encdoc.tmp", STORAGE_DIR);
    
    let mut tmp_output = BufWriter::new(File::create(&tmp_output_path)?);

    let mut buffer = [0u8; BUFFER_SIZE];

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
                .encrypt_last((&buffer[..read_bytes]))
                .map_err(|e| anyhow!("Failed to encrypt last chunk {}", e))?;
            tmp_output.write(&ciphertext)?;
            hasher.update(&ciphertext);
            break;
        }
                
        tmp_output.flush()?;
    }

    // finalize the hash and get the CID
    let cid = hasher.finalize().to_hex().to_string();

    // move the file to the storage folder based on CID
    
    // let final_output = BufWriter::new(File::create(&output_path)?);
    // println!("final_output: {:?}", final_output);
    let storage_path = format!("{:}/{}/{}.bin", STORAGE_DIR, metadata, cid);
    println!("storage_path: {}", storage_path);

    // Ensure the parent directory is created.
    //fs::create_dir_all(&storage_path)?;

    if let Some(parent) = Path::new(&storage_path).parent(){
        fs::create_dir_all(parent)?;
    } else {
        return Err(anyhow!("Failed to created path because of non existing parent"))
    }
    // Move/rename the .tmp file to the final path
    fs::rename(&tmp_output_path, &storage_path)?;

    Ok(cid)
}


async fn decrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    cid: &str,
    metadata: &str,
    output_path: &str
) -> Result<(), Error> {
   
    let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());

    let input_doc_path = format!("{}{}/{}.bin", STORAGE_DIR, metadata, cid);
    let file_enc_content = fs::read(&input_doc_path)?;
    let mut hasher = Hasher::new();
    hasher.update(&file_enc_content);
    let compare_cid = hasher.finalize().to_hex().to_string();

    if compare_cid != cid {
        return Err(anyhow!("File content hash does not match CID"))
    }

    let mut input = BufReader::new(File::open(input_doc_path)?);
    
    let mut output = BufWriter::new(File::create(output_path)?);

    let mut buffer = [0u8; BUFFER_SIZE + 16]; // +16 for tag overhead

    loop {
        let read_bytes = input.read(&mut buffer)?;

        if read_bytes == BUFFER_SIZE {

            let cleartext = stream_decryptor
                .decrypt_next(buffer.as_slice())
                .map_err(|e|anyhow!("Failed to decrypt cleartext chunk {}", e))?;
            output.write(&cleartext)?;

        } else if read_bytes == 0 {
            break

        } else {

            let cleartext = stream_decryptor
                .decrypt_last((&buffer[..read_bytes]))
                .map_err(|e| anyhow!("Failed to decrypt last chunk {}", e))?;
            output.write(&cleartext)?;
            break;
        }
    }
    output.flush()?;
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Error>{

    let (new_key, new_nonce) = newkey().await?;

    let cid = encrypt_file(&new_key, &new_nonce, "../ghost-ground/data/smallstuff.txt", "mission").await?;
    decrypt_file(&new_key, &new_nonce, &cid, "mission", "../content-hashing/data_dec/smallstuff.txt").await?;

    Ok(())

}
