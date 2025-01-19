use std::fs::{self, File};
use std::io::{self, Read, Write, BufReader, BufWriter};

use anyhow::{anyhow, Error, Ok, Result};

use chacha20poly1305::aead::{OsRng, stream::{EncryptorBE32, DecryptorBE32}};
use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
use rand::RngCore;

const BUFFER_SIZE: usize = 64 * 1024; // Define the buffer size for reading and writing data in chunks.

///// CRYPTOGRAPHY SECTION
/// Generate a new encryption key and nonce.
async fn newkey() -> Result<([u8; 32], [u8; 19]), Error> {
    let mut key = [0u8; 32]; // Create a 32-byte array for the key.
    let mut nonce = [0u8; 19]; // Create a 19-byte array for the nonce.

    OsRng.fill_bytes(&mut key); // Fill the key with random bytes.
    OsRng.fill_bytes(&mut nonce); // Fill the nonce with random bytes.
    
    Ok((key, nonce)) // Return the generated key and nonce.
}

/// Encrypt a file using the provided key and nonce.
async fn encrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    input_path: &str,
    output_path: &str) -> Result<(), Error> {

    let cipher = XChaCha20Poly1305::new(key.as_ref().into()); // Initialize the cipher with the key.
    
    let mut stream_encryptor = EncryptorBE32::from_aead(cipher, nonce.as_ref().into()); // Create a streaming encryptor.

    let mut input = BufReader::new(File::open(input_path)?); // Open the input file and wrap it in a buffered reader.
    let mut output = BufWriter::new(File::create(output_path)?); // Create the output file and wrap it in a buffered writer.

    let mut buffer = [0u8; BUFFER_SIZE]; // Buffer to hold chunks of data read from the input file.

    loop {
        let read_bytes = input.read(&mut buffer)?; // Read a chunk of data into the buffer.
        
        if read_bytes == BUFFER_SIZE { // If the buffer is completely filled:
            let ciphertext = stream_encryptor
                .encrypt_next(buffer.as_slice()) // Encrypt the chunk.
                .map_err(|e|anyhow!("Failed to encrypt ciphertext chunk {}", e))?; // Handle errors.
            output.write(&ciphertext)?; // Write the encrypted data to the output file.

        } else { // If this is the last chunk:

            let ciphertext = stream_encryptor
                .encrypt_last((&buffer[..read_bytes])) // Encrypt the final chunk.
                .map_err(|e| anyhow!("Failed to encrypt last chunk {}", e))?; // Handle errors.
            output.write(&ciphertext)?; // Write the encrypted data to the output file.
            break; // Exit the loop.
        }
        output.flush()?; // Flush the buffer to ensure all data is written.
    }
    Ok(()) // Return success.
}

/// Decrypt a file using the provided key and nonce.
async fn decrypt_file(
    key: &[u8; 32],
    nonce: &[u8; 19],
    input_path: &str,
    output_path: &str
) -> Result<(), Error> {
   
    let cipher = XChaCha20Poly1305::new(key.as_ref().into()); // Initialize the cipher with the key.
    
    let mut stream_decryptor = DecryptorBE32::from_aead(cipher, nonce.as_ref().into()); // Create a streaming decryptor.

    let mut input = BufReader::new(File::open(input_path)?); // Open the input file and wrap it in a buffered reader.
    let mut output = BufWriter::new(File::create(output_path)?); // Create the output file and wrap it in a buffered writer.

    let mut buffer = [0u8; BUFFER_SIZE + 16]; // Buffer to hold chunks of data (extra 16 bytes for authentication tag).

    loop {
        let read_bytes = input.read(&mut buffer)?; // Read a chunk of data into the buffer.

        if read_bytes == BUFFER_SIZE { // If the buffer is completely filled:

            let cleartext = stream_decryptor
                .decrypt_next(buffer.as_slice()) // Decrypt the chunk.
                .map_err(|e|anyhow!("Failed to decrypt cleartext chunk {}", e))?; // Handle errors.
            output.write(&cleartext)?; // Write the decrypted data to the output file.

        } else if read_bytes == 0 { // If no more data is read:
            break; // Exit the loop.

        } else { // If this is the last chunk:

            let cleartext = stream_decryptor
                .decrypt_last((&buffer[..read_bytes])) // Decrypt the final chunk.
                .map_err(|e| anyhow!("Failed to decrypt last chunk {}", e))?; // Handle errors.
            output.write(&cleartext)?; // Write the decrypted data to the output file.
            break; // Exit the loop.
        }
    }
    output.flush()?; // Flush the buffer to ensure all data is written.
    Ok(()) // Return success.
}

/// Main function to demonstrate file encryption and decryption.
#[tokio::main]
async fn main() -> Result<(), Error>{

    let (new_key, new_nonce) = newkey().await?; // Generate a new key and nonce.

    // Encrypt the input file and save the output to a specified location.
    encrypt_file(&new_key, &new_nonce, "../ghost-ground/data/smallstuff.txt", "../content-hashing/data_enc/smallstuff.bin").await?;
    
    // Decrypt the encrypted file and save the output to a specified location.
    decrypt_file(&new_key, &new_nonce,  "../content-hashing/data_enc/smallstuff.bin", "../content-hashing/data_dec/smallstuff.txt").await?;

    Ok(()) // Return success.
}
