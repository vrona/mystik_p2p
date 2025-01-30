use std::{any::{Any, TypeId}, fmt::Error, path::Path};
use blake3::{Hash, Hasher as GenuineBlake3Hasher};
use monotree::{
    database::sled::Sled,
    hasher::Blake3,
    verify_proof,
    Hasher,
    Result,
    Errors,
    Monotree
};

const STORAGE_METADATA: &str = "../merkle-meta/metadata_db/";


struct Metadata {
    file_name: String,
    file_type: String,
    mission: String,
    // file_key: Vec<u8>,
    // file_nonce: Vec<u8>,
    // timestamp: u64,
}

impl Metadata {
    fn new(
        file_name: String,
        file_type: String,
        mission: String,
        // file_key: Vec<u8>,
        // file_nonce: Vec<u8>,
        // timestamp: u64,

    ) -> Self {
        Self {
            file_name,
            file_type,
            mission,
            // file_key,
            // file_nonce,

        }
    }

    fn hashing(single_meta: String) -> Hash {
        let mut hasher = GenuineBlake3Hasher::new();
        hasher.update(&single_meta.as_bytes());
        let hash = hasher.finalize();

        return hash
    }

    fn key_hashing(&self) -> Hash {

        let file_name_h = Self::hashing(self.file_name.clone());
        let file_type_h = Self::hashing(self.file_type.clone());
        let mission_h = Self::hashing(self.mission.clone());

        let mut hasher_key = GenuineBlake3Hasher::new();
        
        hasher_key.update(file_name_h.as_bytes());
        hasher_key.update(file_type_h.as_bytes());
        hasher_key.update(mission_h.as_bytes());
        
        let key_hash = hasher_key.finalize();

        return key_hash
    }
}


struct MerkleMetaTree {
    tree: Monotree<Sled>,
    root: Option<[u8; 32]>,
}

impl MerkleMetaTree {

    // init the Monotree instance (database > tree and root)
    fn new() -> Result<Self>{
        
        let tree = Monotree::<Sled, Blake3>::new(STORAGE_METADATA);

        Ok(MerkleMetaTree{
            tree,
            root: None,
        })
    }

    // insert key-value pair into the tree
    fn insert(&mut self, key: &[u8; 32], value: &[u8; 32]) -> Result<()> {
        self.root = self.tree.insert(self.root.as_ref(), &key, &value)?;
        Ok(())
    }

    // remove en entry (aka a key) from the tree
    fn remove(&mut self, key: &[u8]) -> Result<()> {
        self.root = self.tree.remove(self.root.as_ref(), &key)?;
        Ok(())
    }

    // get (or find) a leaf or a value for a given leaf from the tree
    fn get(&mut self, key: &[u8; 32]) -> Result<(), Option<[u8; 32]>> {
        
        match self.tree.get(self.root.as_ref(), &key)? {
            Some(leaf) => Ok(()),
            None => Err(None),
        }
    }

    // generate the Merkle proof for the root and the key
    fn proof(&mut self, key: &[u8; 32]) -> Result<Option<Vec<(bool, Vec<u8>)>>> {
        let proof = self.tree.get_merkle_proof(self.root, &key)?;
        return Ok(proof)
    }

    // generate the Merkle proof for the root and the key
    fn verify_merkle_proof(&mut self, key: &[u8; 32], leaf: &[u8; 32]) -> Result<()> {
        let protocol_hash = GenuineBlake3Hasher::new();
        let proved_key = self.proof(&key)?;
        let verified = verify_proof(&protocol_hash, self.root.as_ref(), &leaf, proved_key)?;
        Ok(())
    }

}

fn main() {
    let file_name = "example.txt".to_string();
    let file_type = "txt".to_string();
    let mission = "mars".to_string();
    // let cid = "bafy...".to_string();
    // let file_key = vec![1, 2, 3, 4];
    // let file_nonce = vec![5, 6, 7, 8];
    // let timestamp = 1670000000;

    let metadata = Metadata::new(file_name, file_type, mission);
   // init Monotree instance with the persistent database (rocksdb) and hasher (blake3)
   let mut tree = Monotree::<Sled, Blake3>::new(STORAGE_METADATA);
   
   //
   // define root hash
   let mut root = [0u8, 32];

   // create a merkle trie using Blake3TrieHasher
   //let mut trie = TrieDBMut::<KeccakHasher>::new(&mut db, &mut root);


}
