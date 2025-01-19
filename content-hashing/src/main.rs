use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};


use anyhow::{anyhow, Error, Ok, Result};

use chacha20poly1305::aead::{OsRng, stream::{EncryptorBE32, DecryptorBE32}};

use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;

const BUFFER_SIZE: usize = 64 * 1024;

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
    output_path: &str) -> Result<(), Error> {

    let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into());

    let mut input = BufReader::new(File::open(input_path)?);
    let mut output = BufWriter::new(File::create(output_path)?);

    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        let read_bytes = input.read(&mut buffer)?;
        
        if read_bytes == BUFFER_SIZE {
            let ciphertext = stream_encryptor
                .encrypt_next(buffer.as_slice())
                .map_err(|e|anyhow!("Failed to encrypt ciphertext chunk {}", e))?;
            output.write(&ciphertext)?;

        } else {

            let ciphertext = stream_encryptor
                .encrypt_last((&buffer[..read_bytes]))
                .map_err(|e| anyhow!("Failed to encrypt last chunk {}", e))?;
            output.write(&ciphertext)?;
            break;
        }
        output.flush()?;
    }
    Ok(())
}


async fn decrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    input_path: &str,
    output_path: &str
) -> Result<(), Error> {
   
    let cipher = XChaCha20Poly1305::new(key.as_ref().into());
    
    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into());


    let mut input = BufReader::new(File::open(input_path)?);
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

    encrypt_file(&new_key, &new_nonce, "../ghost-ground/data/smallstuff.txt", "../content-hashing/data_enc/smallstuff.bin").await?;
    decrypt_file(&new_key, &new_nonce,  "../content-hashing/data_enc/smallstuff.bin", "../content-hashing/data_dec/smallstuff.txt").await?;

    Ok(())

}
