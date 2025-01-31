use std::{any::{Any, TypeId}, path::Path};
use rs_merkle::{self, algorithms::Sha256, Hasher as MerkleHasher};

use monotree::{
    database::sled::Sled,
    hasher::{Blake3, Hasher},
    verify_proof,
    Result,
    Errors,
    Monotree
};

mod rsmerkle;
use rsmerkle::PersistentMetaMerkle;

const STORAGE_MIDCID: &str = "../merkle-meta/midtocid/";


struct Metadata {
    file_name: String,
    file_type: String,
    mission: String,
    // file_key: Vec<u8>,
    // file_nonce: Vec<u8>,
}

impl Metadata {
    fn new(
        file_name: String,
        file_type: String,
        mission: String,
        // file_key: Vec<u8>,
        // file_nonce: Vec<u8>,


    ) -> Self {
        Self {
            file_name,
            file_type,
            mission,
            // file_key,
            // file_nonce,

        }
    }


    // fn hashing(&self, single_meta: String) -> Result<[u8; 32]> {
    //     let hasher = Blake3::new();

    //     let hash = hasher.digest(&single_meta.as_bytes());
    //     return Ok(hash)
    // }

    // Merkle tree
    fn leaf_hashing(&self) -> Result<Vec<[u8; 32]>>{
        let metadata = [&self.file_name, &self.file_type, &self.mission];
        let leaves: Vec<[u8; 32]> = metadata
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();
        return Ok(leaves)
    }

    // fn key_hashing(&self) -> Result<[u8; 32]> {

    //     let leafs = self.leaf_hashing()?;
    //     let hasher_key = Blake3::new();
    //     let mut concatened = Vec::new();
        
    //     for leaf in leafs {
    //         concatened.extend_from_slice(&leaf);
    //     }

    //     let key_hash = hasher_key.digest(&concatened);

    //     return Ok(key_hash)
    // }
}


struct MerkleMetaTree {
    tree: Monotree<Sled, Blake3>,
    root: Option<[u8; 32]>,
}

impl MerkleMetaTree {

    // init the Monotree instance (database > tree and root)
    fn new() -> Result<Self>{
        
        let newtree = Monotree::<Sled, Blake3>::new(STORAGE_MIDCID);

        Ok(MerkleMetaTree{
            tree: newtree,
            root: None,
        })
    }

    // insert key-leaf pair into the tree
    fn insert(&mut self, key: &[u8; 32], leaf: &[u8; 32]) -> Result<()> {
        self.root = self.tree.insert(self.root.as_ref(), &key, &leaf)?;
        Ok(())
    }

    // remove en entry (aka a key) from the tree
    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.root = self.tree.remove(self.root.as_ref(), &key)?;
        Ok(())
    }

    // get (or find) a leaf or a leaf for a given leaf from the tree
    fn get(&mut self, key: &[u8; 32]) -> Result<Option<[u8; 32]>> {
        
        match self.tree.get(self.root.as_ref(), &key)? {
            Some(leaf) => Ok(Some(leaf)),
            None => Err(Errors::new("Failed to get leaf")),
        }
    }

    // generate the Merkle proof for the root and the key
    fn proof(&mut self, key: &[u8; 32]) -> Result<Option<Vec<(bool, Vec<u8>)>>> {
        let proof = self.tree.get_merkle_proof(self.root.as_ref(), key)?;
        return Ok(proof)
    }

    // generate the Merkle proof for the root and the key
    fn verify_merkle_proof(&mut self, key: &[u8; 32], leaf: &[u8; 32]) -> Result<bool> {
        let protocol_hash = Blake3::new();
        let proved_key = self.proof(key)?;
        let verified = verify_proof(&protocol_hash, self.root.as_ref(), &leaf, proved_key.as_ref());
        Ok(verified)
    }

}

fn main() -> Result<()> {
    
    let file_name = "ObserveThis".to_string();
    let file_type = "txt".to_string();
    let mission = "BCX451".to_string();
    // let cid = "bafy...".to_string();
    // let file_key = vec![1, 2, 3, 4];
    // let file_nonce = vec![5, 6, 7, 8];
    // let timestamp = 1670000000;

    let metadata = Metadata::new(file_name, file_type, mission);
    let leafs = metadata.leaf_hashing()?;
    let leafone = leafs[0];
    // for leaf in leafs {
    //     println!("Leafs:\n{:?}", leaf);
    // }
    let key = monotree::utils::random_hash();//metadata.key_hashing();
    //println!("Key:\n{:?}", key);
   // init Monotree instance with the persistent database (rocksdb) and hasher (blake3)
   let mut tree = MerkleMetaTree::new()?;
   tree.insert(&key, &leafone)?;
   
   // define root hash


   // create a merkle trie using Blake3TrieHasher
   //let mut trie = TrieDBMut::<KeccakHasher>::new(&mut db, &mut root);

    Ok(())
}
