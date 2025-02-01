use std::{any::{Any, TypeId}, error::Error, result::Result, path::Path};

use rs_merkle::{self, algorithms::Sha256, Hasher as MerkleHasher, MerkleTree};

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

        let mut merkletree = Some(RSMerkleTree::new(leaves)?);
        
        let persistentstorage = if let Some(tree) = &merkletree {
            Some(PersistentMetaMerkle::new(&cid, tree.clone())?)
        } else {
            None
        };
        
        Ok(Self {
            file_name,
            file_type,
            mission,
            cid,
            merkletree,
            persistentstorage,
            // file_key,
            // file_nonce,
        })
    }

    // Merkle tree
    pub(crate) fn leaf_hashing(&self) -> Result<Vec<[u8; 32]>, Box<dyn Error>>{
        let metadata = [&self.file_name, &self.file_type, &self.mission];
        let leaves: Vec<[u8; 32]> = metadata
            .iter()
            .map(|x| Sha256::hash(x.as_bytes()))
            .collect();
        Ok(leaves)
    }

    // Create tree

    // pub(crate) fn create_tree(&self) -> Result<(), anyhow::Error>{
    //     let leaves = self.leaf_hashing()?;//.expect("Failed to Metadata leaves");
    //     self.merkletree = RSMerkleTree::new(leaves).expect("Failed to init the metadata merkle tree");

    //     let persistent_storage_metadata = PersistentMetaMerkle::new(&self.cid, & mut new_metadata_tree);

    //     Ok(())

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

fn main() -> Result<(), Box<dyn Error>> {
    
    let file_name = "ObserveThis".to_string();
    let file_type = "txt".to_string();
    let mission = "BCX451".to_string();
    let cid = "b5af00meed83xdb47".to_string();


    let metadata = Metadata::new(file_name, file_type, mission, cid)?;
    let leafs = metadata.leaf_hashing()?;
    let leafone = leafs[0];
    // for leaf in leafs {
    //     println!("Leafs:\n{:?}", leaf);
    // }
    let key = monotree::utils::random_hash();//metadata.key_hashing();
    //println!("Key:\n{:?}", key);
   // init Monotree instance with the persistent database (rocksdb) and hasher (blake3)
   let mut tree = MonoMetaTree::new()?;
   tree.insert(&key, &leafone)?;
   
   // define root hash

   // create a merkle trie using Blake3TrieHasher
   //let mut trie = TrieDBMut::<KeccakHasher>::new(&mut db, &mut root);

    Ok(())
}
