use std::{any::{Any, TypeId}, error::Error, result::Result, path::Path};

use rs_merkle::{self, algorithms::Sha256, Hasher as MerkleHasher, MerkleTree, MerkleProof};

use hex::{encode, decode};
use tokio::main;
use monotree::{
    database::sled::Sled,
    hasher::{Blake3, Hasher},
    verify_proof,
    Result as MonoTResult,
    Errors,
    Monotree
};

mod store_rsmerkle;
use store_rsmerkle::{RSMerkleTree, PersistentRsMerkle};

use chacha20poly1305::aead::{OsRng, stream::{EncryptorBE32, DecryptorBE32}};
use rand::RngCore;

const STORAGE_MIDCID: &str = "../merkle-meta/midtocid/";

struct Metadata {
    file_name: String,
    file_type: String,
    mission: String,
    cid: String,
    file_key: Option<Vec<u8>>,
    file_nonce: Option<Vec<u8>>,

}

impl Metadata {
    fn new(
        file_name: String,
        file_type: String,
        mission: String,
        cid: String,
        file_key: Option<Vec<u8>>,
        file_nonce: Option<Vec<u8>>,

    ) -> Self {
        
        Self {
            file_name,
            file_type,
            mission,
            cid,
            file_key,
            file_nonce,
    }
}

    fn create_tree(&mut self) -> Result<String, Box<dyn Error>> {
        
        let leaves = self.leaf_basis_hashing()?;

        let merkletree = RSMerkleTree::new(leaves.clone())?;

        let mut persistentstorage = PersistentRsMerkle::new(None, Some(merkletree))?;

        // TO DELETE -- Store leaves.
        // for (i, leaf) in leaves.iter().enumerate() {
        //     persistentstorage.store_leaf(i, &bincode::serialize(leaf)?)?;
        // }

        // Store tree, and root
        persistentstorage.store_tree()?;
        persistentstorage.store_root()?;
    
        let root_hex = persistentstorage
            .tree
            .as_ref()
            .ok_or("Merkle tree not found")?
            .root_hex()
            .ok_or("Could not retrieve Merkle root")?;
        
        Ok(root_hex)
    }


    // Merkle tree
    pub fn leaf_basis_hashing(&self) -> Result<Vec<[u8; 32]>, Box<dyn Error>> {
        let metadata = [&self.file_name, &self.file_type, &self.mission, &self.cid];
        let leaves: Vec<[u8; 32]> = metadata
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();
        Ok(leaves)
    }

    // Retrieve a root
    fn retrieve_root_hex(&self) -> Result<String, Box<dyn Error>> {
        
        let leaves: Vec<[u8; 32]> = self.leaf_basis_hashing()?;

        let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);
        // let current_root_hex = merkle_tree.root_hex().ok_or("Not able to return current root hex")?;
        // println!("current root hex (internal): {}", current_root_hex);
        let mut persistent_meta_merkle = PersistentRsMerkle::new(None, Some(merkle_tree))?;
        let is_valid = persistent_meta_merkle.verify_tree()?;

        if ! is_valid {
            return Err("Unvalid: stored tree for provided leaves.".into())
        }
        
        let root_hex = persistent_meta_merkle
            .tree
            .as_ref()
            .ok_or("Merkle tree not found")?
            .root_hex()
            .ok_or("Could not retrieve Merkle root")?;
        
        Ok(root_hex)
    }

    // Insert Leaf (new leaf)
    fn insert_leaf(&self, key_or_nonce: &[u8]) -> Result<Option<String>, Box<dyn Error>> {

        let leaf =  Sha256::hash(key_or_nonce);

        let roothex_retrieved = self.retrieve_root_hex()?;
        let mut persistent_rsmerkle = PersistentRsMerkle::new(Some(&roothex_retrieved), None)?;
        let instant_tree = persistent_rsmerkle.load_tree()?;
        
        // Delete current tree folder.
        persistent_rsmerkle.delete_merkle_meta()?;

        if let Some(mut tree) = instant_tree.tree {
            tree.insert(leaf)
            .commit();

           let mut new_persistent_rsmerkle = PersistentRsMerkle::new(None, Some(tree))?;

            new_persistent_rsmerkle.store_tree()?;
            new_persistent_rsmerkle.store_root()?;

            let new_root_hex = new_persistent_rsmerkle
                .tree
                .as_ref()
                .ok_or("Merkle tree not found")?
                .root_hex();
        
            return Ok(new_root_hex)
        } else {
            return Err("Failed to provide the new root hex with inserted leaf".into());
        }
    }
    
    // Append Leaves
    fn append_leaves(&self, file_key: [u8; 32], file_nonce: [u8; 19]) -> Result<Option<String>, Box<dyn Error>> {
       
        let roothex_retrieved = self.retrieve_root_hex()?;
        let mut persistent_rsmerkle = PersistentRsMerkle::new(Some(&roothex_retrieved), None)?;
        let instant_tree = persistent_rsmerkle.load_tree()?;
        
        // Delete current tree folder.
        persistent_rsmerkle.delete_merkle_meta()?;

        let additional_leaves = vec![file_key.to_vec(), file_nonce.to_vec()];
        let mut add_leaves: Vec<[u8; 32]> = additional_leaves
            .iter()
            .map(|x| Sha256::hash(x))
            .collect();
        
        if let Some(mut tree) = instant_tree.tree {
            tree.append(&mut add_leaves)
            .commit();

            let mut new_persistent_rsmerkle = PersistentRsMerkle::new(None, Some(tree))?;

            new_persistent_rsmerkle.store_tree()?;
            new_persistent_rsmerkle.store_root()?;

            let new_root_hex = new_persistent_rsmerkle
                .tree
                .as_ref()
                .ok_or("Merkle tree not found")?
                .root_hex();
            
            return Ok(new_root_hex)
        } else {
            return Err("Failed to provide the new root hex with appended leaves".into());
        }
    }

    // // Proof Leaf
    // pub fn prove_leaf(&self, initial_leaves: &[&str]) -> Result<bool, Box<dyn Error>> {
        
    //     let load_tree = PersistentMetaMerkle::new(cid)?;

    //     let leaves: Vec<[u8; 32]> = initial_leaves
    //         .iter()
    //         .map(|x| Sha256::hash(x.as_bytes()))
    //         .collect();

    //     let indices_to_prove: Vec<usize> = (0..leaves.len()).collect();

    //     let merkle_proof = load_tree.tree.proof(&indices_to_prove);
    //     let merkle_root = load_tree.tree.root().ok_or("can't get current root")?;
    //     // serialize proof to pass it to the client
    //     let proof_bytes = merkle_proof.to_bytes();

    //     // Parse proof back on the client
    //     let proof = MerkleProof::<Sha256>::try_from(proof_bytes)?;

    //     let leave_verify = proof.verify(merkle_root, &indices_to_prove, &leaves, leaves.len());
    //     Ok(leave_verify)
    // }

}


struct MonoMetaTree {
    tree: Monotree<Sled, Blake3>,
    root: Option<[u8; 32]>,
}

impl MonoMetaTree {

    // init the Monotree instance (database > tree and root)
    fn new() -> MonoTResult<Self>{
        
        let newtree = Monotree::<Sled, Blake3>::new(STORAGE_MIDCID);

        Ok(MonoMetaTree{
            tree: newtree,
            root: None,
        })
    }

    // insert key-leaf pair into the tree
    fn insert(&mut self, key: &[u8; 32], leaf: &[u8; 32]) -> MonoTResult<()> {
        self.root = self.tree.insert(self.root.as_ref(), &key, &leaf)?;
        Ok(())
    }

    // remove en entry (aka a key) from the tree
    fn remove(&mut self, key: &[u8]) -> MonoTResult<()> {
        self.root = self.tree.remove(self.root.as_ref(), &key)?;
        Ok(())
    }

    // get (or find) a leaf or a leaf for a given leaf from the tree
    fn get(&mut self, key: &[u8; 32]) -> MonoTResult<Option<[u8; 32]>> {
        
        match self.tree.get(self.root.as_ref(), &key)? {
            Some(leaf) => Ok(Some(leaf)),
            None => Err(Errors::new("Failed to get leaf")),
        }
    }

    // generate the Merkle proof for the root and the key
    fn proof(&mut self, key: &[u8; 32]) -> MonoTResult<Option<Vec<(bool, Vec<u8>)>>> {
        let proof = self.tree.get_merkle_proof(self.root.as_ref(), key)?;
        return Ok(proof)
    }

    // generate the Merkle proof for the root and the key
    fn verify_merkle_proof(&mut self, key: &[u8; 32], leaf: &[u8; 32]) -> MonoTResult<bool> {
        let protocol_hash = Blake3::new();
        let proved_key = self.proof(key)?;
        let verified = verify_proof(&protocol_hash, self.root.as_ref(), &leaf, proved_key.as_ref());
        Ok(verified)
    }

}

/// String to Hex conversion

/// Converts `[u8; 32]` to a hex string
fn bytes32_to_hex(bytes: [u8; 32]) -> String {
    encode(bytes) // Convert bytes to hex string
}

/// Converts a hex string into `[u8; 32]`
fn hex_to_bytes32(hex_str: &str) -> Result<[u8; 32], Box<dyn Error>> {
    let decoded_bytes = decode(hex_str)?; // Decode hex string into Vec<u8>

    // Ensure it's exactly 32 bytes
    let byte_array: [u8; 32] = decoded_bytes.try_into()
        .map_err(|_| "Hex string is not 32 bytes long")?;

    Ok(byte_array)
}


///// CRYPTOGRAPHY SECTION
async fn newkey() -> Result<([u8; 32], [u8; 19]), Box <dyn Error>> {

    let mut key = [0u8; 32];
    let mut nonce = [0u8; 19];

    OsRng.fill_bytes(&mut key);
    OsRng.fill_bytes(&mut nonce);
    
    Ok((key, nonce))
}

#[main]
async fn main() -> Result<(), Box<dyn Error>> {
    
    let file_name = "ObserveThis".to_string();
    let file_type = "txt".to_string();
    let mission = "BCX451".to_string();
    let cid = "b5af00meed83xdb47".to_string();

    let mut metadata = Metadata::new(file_name.clone(), file_type.clone(), mission.clone(), cid.clone(), None, None);
    let roothex_aka_mid = hex_to_bytes32(&metadata.create_tree()?)?;


    //let key = monotree::utils::random_hash();//metadata.key_hashing();
    //println!("Key:\n{:?}", key);
    // init Monotree instance with the persistent database (rocksdb) and hasher (blake3)
    let mut tree = MonoMetaTree::new()?;

    /*
    
    >> IMPORTANT
    To add the "leafone" which should be == to the PeerId we need to extract 32 bytes from the PeerId (= public key)
    
    

        fn peer_id_to_bytes32(peer_id: &PeerId) -> Result<[u8; 32], String> {
        let peer_id_bytes = peer_id.to_bytes(); // Extract `PeerId` as bytes

        // Ensure `PeerId` provides at least 32 bytes
        if peer_id_bytes.len() < 32 {
            return Err("PeerId is too short to extract 32 bytes".to_string());
        }

        // Extract the first 32 bytes
        let extracted_bytes: [u8; 32] = peer_id_bytes[..32]
            .try_into()
            .expect("Slice conversion failed");

        Ok(extracted_bytes)
    }

    >> WARNING
    A "matcher" is missing to do the opposite: match the PeerId for a given leafone (32 bytes)
     */


    tree.insert(&roothex_aka_mid, &leafone)?;

// define root hash

// create a merkle trie using Blake3TrieHasher
//let mut trie = TrieDBMut::<KeccakHasher>::new(&mut db, &mut root);

    /* 
    let current_root_hex = metadata.retrieve_root_hex()?;

    let (new_key, new_nonce) = newkey().await?;

    let _ = metadata.append_leaves(new_key, new_nonce)?;
    */

    Ok(())
}

/* 

*/