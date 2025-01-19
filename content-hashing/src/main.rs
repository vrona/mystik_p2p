use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};
use std::path::Path;

use aead::rand_core::impls::next_u32_via_fill;
///use aes::cipher::generic_array::sequence::GenericSequence;
use aes::cipher::generic_array::GenericArray;
use aes_gcm::{
    aead::{OsRng, stream::{ EncryptorBE32, DecryptorBE32}
    },
    Aes256Gcm, Key, Nonce,KeyInit  
};

use anyhow::{anyhow, Error, Ok, Result};

use rand::{thread_rng, RngCore};

const BUFFER_SIZE: usize = 8 * 1024;

///// CRYPTOGRAPHY SECTION
async fn newkey() -> Result<String, Error> {

    let key = Aes256Gcm::generate_key(&mut thread_rng());
    let hex_key = hex::encode(key);
    
    println!("New AES-256 key (hex-encoded): {}", hex_key);

    Ok(hex_key)
}

/// Encrypt file
async fn encrypt_file(
    key_hex: &str,
    input_path: &str,
    output_path: &str) -> Result<(), Error> { // Vec<u8>

    // 1. decode hex key -> 32 bytes
    let key_bytes = hex::decode(&key_hex).map_err(|e| anyhow!("Failed to decode hex key: {}", e))?;
    if key_bytes.len() != 32 {
        return Err(anyhow!("Key must be 32 bytes (64 hex characters)"));
    }

    // 2. create AES-256-GCM from the 32-byte key
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    // 3. generate random 12-byte nonce because of mismatch between Aes256Gcm::generate_nonce vs EncryptorBE32
    let mut nonce_bytes = [0u8; 7];
    OsRng.fill_bytes(&mut nonce_bytes);
    println!("Nonce length {}", nonce_bytes.len());
    
    // No let nonce: &GenericArray<u8, typenum::U12> = GenericArray::from_slice(&nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // 4. open files (buffered)
    let mut input = BufReader::new(File::open(input_path)?);
    let mut output = BufWriter::new(File::create(output_path)?);

    // 5. write the nonce as the first 12 bytes of the output
    output.write_all(nonce.as_slice())?;

    // 6. create a streaming encryptor
    let mut encryptor = EncryptorBE32::from_aead(cipher, nonce);

    // 7. read data in chunks, encrypt and write
    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let read_bytes = input.read(&mut buffer)?;
        if read_bytes == 0 {
            // Finalizing the stream
            let final_chunk = encryptor.encrypt_last(&[][..]).map_err(|e| anyhow!("Failed to encrypt last chunk: {}", e))?;
            output.write_all(&final_chunk)?;
            break;
        }

        let cyphertext_chunk = encryptor.encrypt_next(&buffer[..read_bytes]).map_err(|e| anyhow!("Failed to encrypt cyphertext chunk: {}", e))?;
        output.write_all(&cyphertext_chunk)?;
        
    }

    output.flush()?;

    Ok(())
}



/// Decrypt a large file (previously encrypted by `encrypt_file`) using AES-256-GCM in streaming mode.
///
/// - `key_hex`: 64-hex-digit string (32 bytes) for AES-256 (must match encryption key).
/// - `input_path`: Path to the ciphertext file (must contain a 12-byte nonce at the start).
/// - `output_path`: Where to write the recovered plaintext.
async fn decrypt_file(
    key_hex: &str,
    input_path: &str,
    output_path: &str,
) -> Result<(), Error> {
   
    // 1. Decode hex key -> 32 bytes
    let key_bytes = hex::decode(key_hex).map_err(|e| anyhow!("Failed to decode hex key: {}", e))?;
    if key_bytes.len() != 32 {
        return Err(anyhow!("Key must be 32 bytes (64 hex characters)"));
    }

    // 2. Create AES-256-GCM
    let key = Key::<Aes256Gcm>::from_slice(&key_bytes);
    let cipher = Aes256Gcm::new(key);

    // 3. Open files (buffered)
    let mut input = BufReader::new(File::open(input_path)?);
    let mut output = BufWriter::new(File::create(output_path)?);

    // 4. Read the first 12 bytes as the nonce
    let mut nonce_bytes = [0u8; 7];
    input.read_exact(&mut nonce_bytes)?;

    let nonce = Nonce::from_slice(&nonce_bytes);

    // 5. Create the streaming decryptor
    let mut decryptor = DecryptorBE32::from_aead(cipher, &nonce);

    // 6. Read in chunks, decrypt, and write
    let mut buffer = [0u8; BUFFER_SIZE + 16]; // +16 for tag overhead
    loop {
        let read_bytes = input.read(&mut buffer)?;
        if read_bytes == 0 {
            // Finalize
            let final_plaintext_chunk = decryptor.decrypt_last(&[][..]).map_err(|e| anyhow!("Failed to decrypt last chunk: {}", e))?;
            output.write_all(&final_plaintext_chunk)?;
            break;
        }
        let plaintext_chunk = decryptor.decrypt_next(&buffer[..read_bytes]).map_err(|e| anyhow!("Failed to decrypt cyphertext chunk: {}", e))?;
        output.write_all(&plaintext_chunk)?;
        
    }

    output.flush()?;
    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Error>{

    let new_key_hex = newkey().await?;

    encrypt_file(&new_key_hex, "../ghost-ground/data/smallstuff.txt", "../content-hashing/data_enc/smallstuff.bin").await?;
    decrypt_file(&new_key_hex, "../content-hashing/data_enc/smallstuff.bin", "../content-hashing/data_dec/smallstuff.txt").await?;

    Ok(())

}
