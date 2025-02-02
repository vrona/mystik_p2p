use std::{any::{Any, TypeId}, error::Error, result::Result, path::Path};

use rs_merkle::{self, algorithms::Sha256, Hasher as MerkleHasher, MerkleTree, MerkleProof};

use monotree::{
    database::sled::Sled,
    hasher::{Blake3, Hasher},
    verify_proof,
    Result as MonoTResult,
    Errors,
    Monotree
};

mod storersmerkle;
use storersmerkle::{RSMerkleTree, PersistentMetaMerkle};

const STORAGE_MIDCID: &str = "../merkle-meta/midtocid/";

struct Metadata {
    file_name: String,
    file_type: String,
    mission: String,
    cid: String,
    merkletree: Option<MerkleTree<Sha256>>,
    persistentstorage: Option<PersistentMetaMerkle>,
    // file_key: Vec<u8>,
    // file_nonce: Vec<u8>,

}

impl Metadata {
    fn new(
        file_name: String,
        file_type: String,
        mission: String,
        cid: String,
        // file_key: Vec<u8>,
        // file_nonce: Vec<u8>

    ) -> Result<Self, Box<dyn Error>> {
        
        let metadata = Metadata {
            file_name: file_name.clone(),
            file_type: file_type.clone(),
            mission: mission.clone(),
            cid: cid.clone(),
            merkletree: None,
            persistentstorage: None,
        };
        let leaves = metadata.leaf_hashing()?;

        let merkletree = Some(RSMerkleTree::new(leaves.clone())?);
        
        let persistentstorage = if let Some(tree) = &merkletree {
            Some(PersistentMetaMerkle::new(tree.clone())?)

        } else {
            None
        };

       let mut instance = Self {
            file_name,
            file_type,
            mission,
            cid,
            merkletree,
            persistentstorage,
        };

        // Store leaves, tree, and root
        if let Some(ref mut storage) = instance.persistentstorage {
            for (i, leaf) in leaves.iter().enumerate() {
                storage.store_leaf(i, &bincode::serialize(leaf)?)?;
            }
            storage.store_tree()?;
            storage.store_root()?;
        }

        Ok(instance)
    }

    // Merkle tree
    pub fn leaf_hashing(&self) -> Result<Vec<[u8; 32]>, Box<dyn Error>> {
        let metadata = [&self.file_name, &self.file_type, &self.mission];
        let leaves: Vec<[u8; 32]> = metadata
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();
        Ok(leaves)
    }

    // Retrieve a root
    fn retrieve_root_hex(initial_leaves: &[String]) -> Result<Option<String>, Box<dyn Error>> {
        
        let leaves: Vec<[u8; 32]> = initial_leaves
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();

        let merkle_tree = MerkleTree::<Sha256>::from_leaves(&leaves);

        let root_hex = merkle_tree.root_hex();
        Ok(root_hex)
    }


    // Insert Leaf (new leaf)
    fn insert_leaf(initial_leaves: &[String], str_leaf: &str) -> Result<Option<String>, Box<dyn Error>> {

        let leaf =  Sha256::hash(str_leaf.as_bytes());

        if let Some(root_hex) = Self::retrieve_root_hex(initial_leaves)?{
            let mut load_tree = PersistentMetaMerkle::load_tree(&root_hex)?;
            
            load_tree.tree
            .insert(leaf)
            .commit();

        let new_root_hex = load_tree.tree.root_hex();
        
        return Ok(new_root_hex)
        } else {
            return Err("Failed to provide the new root hex with inserted leaf".into());
        }
    }
    
    // Append Leaves
    fn append_leaves(initial_leaves: &[String], additional_leaves: &[&str]) -> Result<Option<String>, Box<dyn Error>> {

        let mut leaves: Vec<[u8; 32]> = additional_leaves
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();
        
        if let Some(root_hex) = Self::retrieve_root_hex(initial_leaves)?{
            let mut load_tree = PersistentMetaMerkle::load_tree(&root_hex)?;

        load_tree.tree
            .append(&mut leaves)
            .commit();

        let new_root_hex = load_tree.tree.root_hex();
        
        return Ok(new_root_hex)
        } else {
            return Err("Failed to provide the new root hex with appended leaves".into());
        }
    }

    
    // Proof Leaf
    // Proof to be enhance because the user is not supposed to know the CID
    // pub(crate) fn leaves_to_prove(&self, cid: &str, initial_leaves: &[&str]) -> Result<bool, Box<dyn Error>> {
    //     let load_tree = PersistentMetaMerkle::load_tree(cid)?;

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

    // Root Hex

    // 

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

fn main() -> Result<(), Box<dyn Error>> {
    
    let file_name = "ObserveThis".to_string();
    let file_type = "txt".to_string();
    let mission = "BCX451".to_string();
    let cid = "b5af00meed83xdb47".to_string();

    let metadata = Metadata::new(file_name.clone(), file_type.clone(), mission.clone(), cid.clone())?;

    let raw_metadata = [file_name, file_type, mission];
    let current_root_hex = Metadata::retrieve_root_hex(raw_metadata.as_ref())?;

    //let leafone = leafs[0];

    /* 
    let key = monotree::utils::random_hash();//metadata.key_hashing();
    //println!("Key:\n{:?}", key);
   // init Monotree instance with the persistent database (rocksdb) and hasher (blake3)
   let mut tree = MonoMetaTree::new()?;
   tree.insert(&key, &leafone)?;
   
   // define root hash

   // create a merkle trie using Blake3TrieHasher
   //let mut trie = TrieDBMut::<KeccakHasher>::new(&mut db, &mut root);
    */

    Ok(())
}
